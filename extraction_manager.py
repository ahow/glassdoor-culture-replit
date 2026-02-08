"""
Extraction Manager - Background sector-by-sector extraction with pause/resume support.
Reads companies from extraction_queue, searches Glassdoor, and extracts all reviews.
Uses database-backed control table for cross-worker compatibility (gunicorn).
"""

import os
import sys
import csv
import json
import time
import logging
import threading
import requests
import psycopg2
from psycopg2.extras import Json
from datetime import datetime
from extraction_openweb import OpenWebNinjaExtractor, get_db_connection, get_db_url

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

OPENWEB_BASE_URL = "https://api.openwebninja.com/realtime-glassdoor-data"
RAPIDAPI_HOST = "real-time-glassdoor-data.p.rapidapi.com"
RAPIDAPI_BASE_URL = f"https://{RAPIDAPI_HOST}"

SECTOR_ORDER = [
    'Financials',
    'Industrials',
    'Information Technology',
    'Health Care',
    'Consumer Discretionary',
    'Consumer Staples',
    'Energy',
    'Materials',
    'Communication Services',
    'Utilities',
    'Real Estate'
]


def init_extraction_control():
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS extraction_control (
                id INTEGER PRIMARY KEY DEFAULT 1,
                command VARCHAR(20) DEFAULT 'idle',
                current_company VARCHAR(255),
                current_sector VARCHAR(255),
                updated_at TIMESTAMP DEFAULT NOW(),
                CONSTRAINT single_row CHECK (id = 1)
            )
        """)
        conn.commit()
        cur.execute("INSERT INTO extraction_control (id, command) VALUES (1, 'idle') ON CONFLICT (id) DO NOTHING")
        conn.commit()

        cur.execute("UPDATE extraction_queue SET status = 'pending' WHERE status = 'extracting'")
        reset_extracting = cur.rowcount
        if reset_extracting > 0:
            logger.info(f"Startup cleanup: reset {reset_extracting} stuck 'extracting' entries to pending")
        
        cur.execute("SELECT command FROM extraction_control WHERE id = 1")
        ctrl_row = cur.fetchone()
        if ctrl_row and ctrl_row[0] in ('running', 'paused'):
            cur.execute("UPDATE extraction_control SET command = 'idle', current_company = NULL, current_sector = NULL WHERE id = 1")
            logger.info(f"Startup cleanup: reset stale '{ctrl_row[0]}' command to idle (thread lost on restart)")
        conn.commit()

        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error initializing extraction_control: {e}")


def _get_db_command():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT command FROM extraction_control WHERE id = 1")
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row[0] if row else 'idle'
    except Exception:
        return 'idle'


def _set_db_command(command, current_company=None, current_sector=None):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE extraction_control 
            SET command = %s, current_company = %s, current_sector = %s, updated_at = NOW()
            WHERE id = 1
        """, (command, current_company, current_sector))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error setting extraction command: {e}")


class ExtractionManager:
    _instance = None
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def __init__(self):
        self._thread = None

    @property
    def is_running(self):
        db_cmd = _get_db_command()
        if db_cmd in ('running', 'paused'):
            return True
        if self._thread is not None and self._thread.is_alive():
            return True
        return False

    @property
    def is_paused(self):
        return _get_db_command() == 'paused'

    def get_status(self):
        try:
            conn = get_db_connection()
            cur = conn.cursor()

            cur.execute("SELECT command, current_company, current_sector FROM extraction_control WHERE id = 1")
            ctrl = cur.fetchone()
            db_command = ctrl[0] if ctrl else 'idle'
            db_company = ctrl[1] if ctrl else None
            db_sector = ctrl[2] if ctrl else None

            cur.execute("""
                SELECT gics_sector,
                       COUNT(*) as total,
                       COUNT(*) FILTER (WHERE status = 'completed') as completed,
                       COUNT(*) FILTER (WHERE status = 'extracting') as extracting,
                       COUNT(*) FILTER (WHERE status = 'failed') as failed,
                       COUNT(*) FILTER (WHERE status = 'no_match') as no_match,
                       COUNT(*) FILTER (WHERE status = 'pending') as pending,
                       COUNT(*) FILTER (WHERE status = 'skipped') as skipped,
                       COALESCE(SUM(reviews_extracted), 0) as total_reviews
                FROM extraction_queue
                GROUP BY gics_sector
                ORDER BY gics_sector
            """)
            
            sectors = {}
            for row in cur.fetchall():
                sectors[row[0]] = {
                    'total': row[1],
                    'completed': row[2],
                    'extracting': row[3],
                    'failed': row[4],
                    'no_match': row[5],
                    'pending': row[6],
                    'skipped': row[7],
                    'total_reviews': row[8]
                }

            cur.execute("""
                SELECT COUNT(*) as total,
                       COUNT(*) FILTER (WHERE status = 'completed') as completed,
                       COUNT(*) FILTER (WHERE status = 'failed') as failed,
                       COUNT(*) FILTER (WHERE status = 'no_match') as no_match,
                       COUNT(*) FILTER (WHERE status = 'pending') as pending,
                       COUNT(*) FILTER (WHERE status = 'extracting') as extracting,
                       COUNT(*) FILTER (WHERE status = 'skipped') as skipped,
                       COALESCE(SUM(reviews_extracted), 0) as queue_reviews
                FROM extraction_queue
            """)
            totals = cur.fetchone()

            cur.execute("SELECT COUNT(*) FROM reviews")
            actual_review_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(DISTINCT company_name) FROM reviews")
            companies_with_reviews = cur.fetchone()[0]

            cur.close()
            conn.close()

            is_running = db_command in ('running', 'paused')
            is_paused = db_command == 'paused'

            ordered_sectors = []
            for s in SECTOR_ORDER:
                if s in sectors:
                    ordered_sectors.append({'name': s, **sectors[s]})

            return {
                'is_running': is_running,
                'is_paused': is_paused,
                'current_company': db_company,
                'current_sector': db_sector,
                'sectors': ordered_sectors,
                'totals': {
                    'total': totals[0],
                    'completed': totals[1],
                    'failed': totals[2],
                    'no_match': totals[3],
                    'pending': totals[4],
                    'extracting': totals[5],
                    'skipped': totals[6],
                    'total_reviews': actual_review_count,
                    'companies_with_reviews': companies_with_reviews
                }
            }
        except Exception as e:
            logger.error(f"Error getting status: {e}")
            return {'error': str(e)}

    def get_sector_companies(self, sector):
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT id, issuer_name, issuer_ticker, isin, country,
                       gics_industry, gics_sub_industry,
                       glassdoor_name, glassdoor_id, status,
                       reviews_extracted, review_count_glassdoor,
                       match_confidence, error_message,
                       started_at, completed_at
                FROM extraction_queue
                WHERE gics_sector = %s
                ORDER BY status DESC, issuer_name
            """, (sector,))
            
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            cur.close()
            conn.close()
            
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.error(f"Error getting sector companies: {e}")
            return []

    def start(self, start_sector=None):
        db_cmd = _get_db_command()
        if db_cmd == 'paused':
            thread_alive = self._thread is not None and self._thread.is_alive()
            if thread_alive:
                _set_db_command('running')
                logger.info("Extraction resumed via DB command (thread still alive)")
                return {'status': 'resumed'}
            else:
                logger.info("Extraction was paused but thread is dead - starting fresh thread")
                _set_db_command('running')
                self._thread = threading.Thread(
                    target=self._run_extraction,
                    args=(start_sector,),
                    daemon=True
                )
                self._thread.start()
                return {'status': 'resumed'}
        if db_cmd == 'running':
            thread_alive = self._thread is not None and self._thread.is_alive()
            if thread_alive:
                return {'status': 'already_running'}
            else:
                logger.info("Command was running but thread is dead - starting fresh thread")
                self._thread = threading.Thread(
                    target=self._run_extraction,
                    args=(start_sector,),
                    daemon=True
                )
                self._thread.start()
                return {'status': 'started'}

        _set_db_command('running')

        self._thread = threading.Thread(
            target=self._run_extraction,
            args=(start_sector,),
            daemon=True
        )
        self._thread.start()
        logger.info(f"Extraction started (sector: {start_sector or 'all'})")
        return {'status': 'started'}

    def pause(self):
        db_cmd = _get_db_command()
        if db_cmd != 'running':
            return {'status': 'not_running'}
        _set_db_command('paused')
        logger.info("Extraction paused via DB command")
        return {'status': 'paused'}

    def stop(self):
        db_cmd = _get_db_command()
        if db_cmd not in ('running', 'paused'):
            _set_db_command('stop_requested')
            time.sleep(0.5)
            _set_db_command('idle')
            return {'status': 'stopped'}
        _set_db_command('stop_requested')
        logger.info("Extraction stop requested via DB command")
        return {'status': 'stopped'}

    def _check_should_stop(self):
        cmd = _get_db_command()
        return cmd in ('stop_requested', 'idle')

    def _check_should_pause(self):
        return _get_db_command() == 'paused'

    def _resolve_isin_name(self, isin):
        if not isin or len(isin) < 10:
            return None
        try:
            resp = requests.post(
                'https://api.openfigi.com/v3/mapping',
                json=[{"idType": "ID_ISIN", "idValue": isin}],
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            if resp.status_code == 200:
                results = resp.json()
                if results and isinstance(results, list) and len(results) > 0:
                    data = results[0].get('data', [])
                    if data and len(data) > 0:
                        name = data[0].get('name', '')
                        if name:
                            name = name.split('-')[0].strip()
                            logger.info(f"ISIN {isin} resolved via OpenFIGI to: {name}")
                            return name
            elif resp.status_code == 429:
                logger.warning(f"OpenFIGI rate limited for ISIN {isin}")
                time.sleep(2)
        except Exception as e:
            logger.warning(f"ISIN lookup failed for {isin}: {e}")
        return None

    def _search_glassdoor(self, company_name, ticker=None, isin=None):
        headers = None
        url = None

        api_key = os.environ.get('OPENWEB_NINJA_API')
        if api_key:
            headers = {'x-api-key': api_key}
            url = f"{OPENWEB_BASE_URL}/company-search"
        else:
            keys = [os.environ.get('RAPIDAPI_KEY_1'), os.environ.get('RAPIDAPI_KEY_2'), os.environ.get('RAPIDAPI_KEY')]
            key = next((k for k in keys if k), None)
            if key:
                headers = {'x-rapidapi-key': key, 'x-rapidapi-host': RAPIDAPI_HOST}
                url = f"{RAPIDAPI_BASE_URL}/company-search"

        if not headers:
            raise Exception("No API keys configured")

        search_names = [company_name]

        isin_name = self._resolve_isin_name(isin)
        if isin_name and isin_name.lower().strip() != company_name.lower().strip():
            search_names.append(isin_name)

        all_results = []
        seen_ids = set()

        for name in search_names:
            try:
                response = requests.get(url, headers=headers, params={'query': name}, timeout=15)
                response.raise_for_status()
                results = response.json().get('data', [])
                for r in results:
                    rid = r.get('company_id') or r.get('id') or r.get('name')
                    if rid not in seen_ids:
                        seen_ids.add(rid)
                        all_results.append(r)
                time.sleep(0.3)
            except Exception as e:
                logger.error(f"Search error for '{name}': {e}")

        if not all_results and ticker:
            try:
                time.sleep(0.3)
                response = requests.get(url, headers=headers, params={'query': ticker}, timeout=15)
                response.raise_for_status()
                results = response.json().get('data', [])
                for r in results:
                    rid = r.get('company_id') or r.get('id') or r.get('name')
                    if rid not in seen_ids:
                        seen_ids.add(rid)
                        all_results.append(r)
            except Exception as e:
                logger.error(f"Search error for ticker '{ticker}': {e}")

        return all_results, isin_name

    def _pick_best_match(self, search_results, issuer_name, ticker, isin_name=None):
        if not search_results:
            return None, 'none'

        issuer_lower = issuer_name.lower().strip()
        ticker_lower = (ticker or '').lower().strip()
        isin_lower = (isin_name or '').lower().strip()
        filler = {'inc', 'inc.', 'corp', 'corp.', 'corporation', 'company', 'the', 'co', 'co.',
                  'ltd', 'ltd.', 'plc', 'group', 'holdings', 'holding', 'sa', 'se', 'ag', 'nv',
                  'limited', '&', 'of', 'de', 'and', 'n.v.', 'n.v', 'ab', 'as', 'a/s', 'asa',
                  'oyj', 'tbk', 'pt', 'bhd', 'berhad', 'pjsc', 'sjsc', 'jsc', 'public',
                  'anonim', 'sirketi', 'ortakligi', 'turk', 'bank', 'financial', 'services',
                  'insurance', 'international', 'global', 'management', 'investment', 'investments',
                  'capital', 'asset', 'fund', 'trust', 'advisors', 'partners', 'bancorp',
                  'national', 'first', 'new', 'american', 'india', 'china'}

        def clean_words(text):
            return set(text.replace(',', '').replace('.', '').replace('-', ' ').lower().split())

        def meaningful_words(words):
            return words - filler

        def calc_overlap(ref_text, candidate_name):
            ref_words = meaningful_words(clean_words(ref_text))
            cand_words = meaningful_words(clean_words(candidate_name))
            if not ref_words or not cand_words:
                return 0
            common = ref_words & cand_words
            return min(len(common) / len(ref_words), len(common) / len(cand_words))

        reference_names = [issuer_lower]
        if isin_lower and isin_lower != issuer_lower:
            reference_names.append(isin_lower)

        for r in search_results:
            name = (r.get('name') or '').lower().strip()
            for ref in reference_names:
                if name == ref:
                    logger.info(f"Exact match: '{name}' == '{ref}'")
                    return r, 'exact'

        best_match = None
        best_overlap = 0
        best_ref = ''
        for r in search_results:
            name = (r.get('name') or '').lower().strip()
            for ref in reference_names:
                overlap = calc_overlap(ref, name)
                if overlap > best_overlap:
                    best_overlap = overlap
                    best_match = r
                    best_ref = ref

        if best_match and best_overlap >= 0.5:
            logger.info(f"High confidence match for '{issuer_name}' (ref='{best_ref}'): '{best_match.get('name')}' (overlap={best_overlap:.2f})")
            return best_match, 'high'

        if ticker_lower and len(ticker_lower) >= 3:
            for r in search_results:
                name = (r.get('name') or '').lower()
                name_words_set = clean_words(name)
                if ticker_lower in name_words_set:
                    return r, 'medium'

        if best_match and best_overlap >= 0.3:
            return best_match, 'low'

        logger.info(f"Rejecting all {len(search_results)} search results for '{issuer_name}' (isin_name='{isin_name}') - best overlap: {best_overlap:.2f}")
        return None, 'none'

    def _update_queue_status(self, queue_id, status, **kwargs):
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            
            sets = ["status = %s", "updated_at = NOW()"]
            vals = [status]
            
            for key, val in kwargs.items():
                sets.append(f"{key} = %s")
                vals.append(val)
            
            vals.append(queue_id)
            cur.execute(f"UPDATE extraction_queue SET {', '.join(sets)} WHERE id = %s", vals)
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            logger.error(f"Error updating queue status: {e}")

    def _run_extraction(self, start_sector=None):
        logger.info("Extraction worker thread started")

        try:
            sectors_to_process = SECTOR_ORDER[:]
            if start_sector and start_sector in sectors_to_process:
                idx = sectors_to_process.index(start_sector)
                sectors_to_process = sectors_to_process[idx:]

            for sector in sectors_to_process:
                if self._check_should_stop():
                    break

                _set_db_command('running', current_sector=sector)
                logger.info(f"=== Starting sector: {sector} ===")

                try:
                    conn = get_db_connection()
                    cur = conn.cursor()
                    cur.execute("""
                        SELECT id, issuer_name, issuer_ticker, isin, country,
                               gics_industry, gics_sub_industry
                        FROM extraction_queue
                        WHERE gics_sector = %s AND status IN ('pending', 'failed')
                        ORDER BY issuer_name
                    """, (sector,))
                    companies = cur.fetchall()
                    cur.close()
                    conn.close()
                except Exception as e:
                    logger.error(f"Error loading sector {sector}: {e}")
                    continue

                logger.info(f"Sector {sector}: {len(companies)} companies to process")

                for company in companies:
                    if self._check_should_stop():
                        break

                    while self._check_should_pause() and not self._check_should_stop():
                        time.sleep(1)

                    if self._check_should_stop():
                        break

                    q_id, issuer_name, ticker, isin, country, industry, sub_industry = company
                    _set_db_command('running', current_company=issuer_name, current_sector=sector)

                    try:
                        self._process_company(q_id, issuer_name, ticker, isin, country,
                                             sector, industry, sub_industry)
                    except Exception as e:
                        logger.error(f"Error processing {issuer_name}: {e}")
                        self._update_queue_status(q_id, 'failed', error_message=str(e)[:500])

                    time.sleep(0.3)

                logger.info(f"=== Completed sector: {sector} ===")
        finally:
            _set_db_command('idle')
            logger.info("Extraction worker thread finished")

    def _process_company(self, q_id, issuer_name, ticker, isin, country,
                         sector, industry, sub_industry):
        logger.info(f"Processing: {issuer_name} ({ticker}, ISIN: {isin})")
        self._update_queue_status(q_id, 'searching', started_at=datetime.now())

        search_results, isin_name = self._search_glassdoor(issuer_name, ticker, isin=isin)
        time.sleep(0.5)

        match, confidence = self._pick_best_match(search_results, issuer_name, ticker, isin_name=isin_name)

        self._update_queue_status(
            q_id, 'searching',
            search_results=Json(search_results[:5]) if search_results else None,
            match_confidence=confidence
        )

        if not match or confidence == 'none':
            logger.warning(f"No Glassdoor match for {issuer_name}")
            self._update_queue_status(q_id, 'no_match',
                                      error_message='No matching company found on Glassdoor')
            return

        if confidence == 'low':
            glassdoor_candidate = match.get('name', '?')
            logger.warning(f"Low confidence match for {issuer_name} -> {glassdoor_candidate} — skipping to avoid wrong company")
            self._update_queue_status(q_id, 'no_match',
                                      error_message=f'Low confidence match rejected: {glassdoor_candidate}')
            return

        glassdoor_name = match.get('name', issuer_name)
        glassdoor_id = match.get('company_id') or match.get('id')
        glassdoor_url = match.get('company_link') or match.get('reviews_link') or match.get('url', '')

        if not glassdoor_id:
            logger.warning(f"No Glassdoor ID for {issuer_name}")
            self._update_queue_status(q_id, 'no_match',
                                      error_message='Search returned result without company ID')
            return

        self._update_queue_status(
            q_id, 'extracting',
            glassdoor_name=glassdoor_name,
            glassdoor_id=glassdoor_id,
            glassdoor_url=glassdoor_url
        )

        logger.info(f"Matched {issuer_name} -> {glassdoor_name} (ID: {glassdoor_id}, confidence: {confidence})")

        extractor = OpenWebNinjaExtractor(
            company_name=glassdoor_name,
            company_id=glassdoor_id,
            glassdoor_url=glassdoor_url,
            gics_sector=sector,
            gics_industry=industry,
            gics_sub_industry=sub_industry,
            isin=isin,
            country=country,
            issuer_name=issuer_name
        )

        success = extractor.extract_all_reviews(sort='MOST_RECENT')

        if success:
            reviews_saved = extractor.new_reviews_saved
            review_count = extractor.metadata.get('review_count', 0)
            self._update_queue_status(
                q_id, 'completed',
                reviews_extracted=reviews_saved,
                review_count_glassdoor=review_count,
                completed_at=datetime.now()
            )
            logger.info(f"Completed {issuer_name}: {reviews_saved} reviews extracted")
        else:
            self._update_queue_status(q_id, 'failed',
                                      error_message='Extraction failed - see extraction_failures table',
                                      reviews_extracted=extractor.new_reviews_saved)
            logger.error(f"Failed extraction for {issuer_name}")

    def retry_company(self, queue_id):
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("UPDATE extraction_queue SET status = 'pending', error_message = NULL WHERE id = %s", (queue_id,))
            conn.commit()
            cur.close()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Error retrying company: {e}")
            return False

    def retry_sector(self, sector):
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                UPDATE extraction_queue 
                SET status = 'pending', error_message = NULL 
                WHERE gics_sector = %s AND status IN ('failed', 'no_match')
            """, (sector,))
            updated = cur.rowcount
            conn.commit()
            cur.close()
            conn.close()
            return updated
        except Exception as e:
            logger.error(f"Error retrying sector: {e}")
            return 0

    def skip_company(self, queue_id):
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("UPDATE extraction_queue SET status = 'skipped' WHERE id = %s", (queue_id,))
            conn.commit()
            cur.close()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Error skipping company: {e}")
            return False

    def update_glassdoor_match(self, queue_id, glassdoor_name, glassdoor_id):
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                UPDATE extraction_queue 
                SET glassdoor_name = %s, glassdoor_id = %s, 
                    match_confidence = 'manual', status = 'pending',
                    error_message = NULL
                WHERE id = %s
            """, (glassdoor_name, glassdoor_id, queue_id))
            conn.commit()
            cur.close()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Error updating match: {e}")
            return False
