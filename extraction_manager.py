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
from culture_scoring import score_review_with_dictionary

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


def init_extraction_control(is_worker=False):
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

        use_worker = os.environ.get('USE_WORKER', '').lower() in ('true', '1', 'yes')
        if not is_worker and not use_worker:
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

    def _use_worker_dyno(self):
        return os.environ.get('USE_WORKER', '').lower() in ('true', '1', 'yes')

    def start(self, start_sector=None):
        db_cmd = _get_db_command()

        if self._use_worker_dyno():
            if db_cmd == 'paused':
                _set_db_command('running')
                logger.info("Extraction resumed via DB command (worker dyno will pick up)")
                return {'status': 'resumed'}
            if db_cmd == 'running':
                return {'status': 'already_running'}
            _set_db_command('running', current_sector=start_sector)
            logger.info(f"Extraction command set to running (worker dyno will execute, sector: {start_sector or 'all'})")
            return {'status': 'started'}

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
        # Build prioritised list of API configs to try: OpenWeb Ninja first, then each RapidAPI key
        api_configs = []
        openweb_key = os.environ.get('OPENWEB_NINJA_API')
        if openweb_key:
            api_configs.append({
                'headers': {'x-api-key': openweb_key},
                'url': f"{OPENWEB_BASE_URL}/company-search",
                'label': 'OpenWebNinja',
            })
        for env_var in ('RAPIDAPI_KEY_1', 'RAPIDAPI_KEY_2', 'RAPIDAPI_KEY'):
            k = os.environ.get(env_var)
            if k:
                api_configs.append({
                    'headers': {'x-rapidapi-key': k, 'x-rapidapi-host': RAPIDAPI_HOST},
                    'url': f"{RAPIDAPI_BASE_URL}/company-search",
                    'label': env_var,
                })

        if not api_configs:
            raise Exception("No API keys configured")

        search_names = [company_name]

        isin_name = self._resolve_isin_name(isin)
        if isin_name and isin_name.lower().strip() != company_name.lower().strip():
            search_names.append(isin_name)

        legal_suffixes = {'inc', 'inc.', 'corp', 'corp.', 'corporation', 'company', 'co', 'co.',
                          'ltd', 'ltd.', 'plc', 'group', 'holdings', 'holding', 'sa', 'se', 'ag', 'nv',
                          'limited', 'n.v.', 'n.v', 'ab', 'as', 'a/s', 'asa', 'oyj', 'tbk', 'pt',
                          'bhd', 'berhad', 'pjsc', 'sjsc', 'jsc', 'public', 'anonim', 'sirketi',
                          'ortakligi', 'the', '&', 'of', 'de', 'and'}
        cleaned = ' '.join(w for w in company_name.split() if w.lower().strip('.') not in legal_suffixes and w.lower() not in legal_suffixes)
        if cleaned and cleaned.lower() != company_name.lower() and len(cleaned) >= 3:
            search_names.append(cleaned)

        def _parse_search_response(resp_json):
            """Parse company list from either RapidAPI (flat) or OpenWeb Ninja (GraphQL nested) format.
            Raises RuntimeError if the upstream API returned a 503/service-unavailable error."""
            raw = resp_json.get('data', [])
            companies = []
            service_errors = []

            for outer in raw:
                if isinstance(outer, dict):
                    # RapidAPI flat format: data is a list of company dicts
                    companies.append(outer)
                elif isinstance(outer, list):
                    # OpenWeb Ninja GraphQL format: data is [[{data:{...}, errors:[...]}]]
                    for item in outer:
                        if not isinstance(item, dict):
                            continue
                        errors = item.get('errors', [])
                        for err in errors:
                            code = err.get('extensions', {}).get('http', {}).get('status')
                            reason = err.get('extensions', {}).get('reason', '')
                            if code in (503, 429) or '503' in str(reason) or '429' in str(reason):
                                service_errors.append(f"HTTP {code}: {reason}")
                        inner_data = item.get('data', {})
                        # directHitCompany
                        dc = inner_data.get('directHitCompany') or {}
                        emp = dc.get('employer')
                        if emp and isinstance(emp, dict) and emp.get('id'):
                            companies.append({
                                'company_id': str(emp['id']),
                                'name': emp.get('name', ''),
                                'overall_rating': emp.get('overallRating'),
                                'review_count': emp.get('reviewCount'),
                            })
                        # employerNameCompaniesData
                        ec = inner_data.get('employerNameCompaniesData') or {}
                        for emp in (ec.get('employers') or []):
                            if isinstance(emp, dict) and emp.get('id'):
                                companies.append({
                                    'company_id': str(emp['id']),
                                    'name': emp.get('name', ''),
                                    'overall_rating': emp.get('overallRating'),
                                    'review_count': emp.get('reviewCount'),
                                })

            if not companies and service_errors:
                raise RuntimeError(f"Glassdoor API service error: {'; '.join(service_errors[:2])}")
            return companies

        def _search_one(query):
            """Try each API config in priority order for a single query string.
            Returns list of company dicts. Raises RuntimeError only if ALL configs
            return service errors and none returned usable results."""
            last_service_error = None
            for cfg in api_configs:
                try:
                    resp = requests.get(cfg['url'], headers=cfg['headers'],
                                        params={'query': query}, timeout=15)
                    resp.raise_for_status()
                    results = _parse_search_response(resp.json())
                    if results:
                        logger.info(f"Search '{query}' via {cfg['label']}: {len(results)} results")
                        return results
                    # Empty results (not a service error) — try next config
                    logger.info(f"Search '{query}' via {cfg['label']}: 0 results, trying next API")
                except RuntimeError as e:
                    last_service_error = e
                    logger.warning(f"Search '{query}' via {cfg['label']} service error: {e} — trying fallback")
                except Exception as e:
                    logger.error(f"Search '{query}' via {cfg['label']} error: {e}")
            if last_service_error:
                raise last_service_error
            return []

        all_results = []
        seen_ids = set()

        queries = list(search_names)
        if not all_results and ticker:
            queries.append(ticker)

        for query in queries:
            results = _search_one(query)
            for r in results:
                rid = r.get('company_id') or r.get('id') or r.get('name')
                if rid not in seen_ids:
                    seen_ids.add(rid)
                    all_results.append(r)
            if all_results:
                break  # Got results — no need to try more query variants
            time.sleep(0.3)

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

        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT company_name, COUNT(*) FROM reviews GROUP BY company_name")
            existing_companies = {row[0].lower(): (row[0], row[1]) for row in cur.fetchall()}
            cur.close()
            conn.close()
        except Exception:
            existing_companies = {}

        _existing_filler = {'inc', 'corp', 'corporation', 'company', 'the', 'ltd', 'plc', 'group',
                            'holdings', 'holding', 'sa', 'se', 'ag', 'nv', 'limited', '&', 'of',
                            'and', 'co', 'international', 'global', 'services', 'financial',
                            'management', 'capital', 'partners', 'investments', 'investment',
                            'asset', 'trust', 'fund', 'national', 'bank', 'insurance', 'de', 'ab'}
        issuer_words = set(issuer_name.lower().replace(',', '').replace('.', '').replace('-', ' ').split())
        issuer_meaningful = issuer_words - _existing_filler
        for comp_lower, (comp_name, rev_count) in existing_companies.items():
            comp_words = set(comp_lower.replace(',', '').replace('.', '').replace('-', ' ').split())
            comp_meaningful = comp_words - _existing_filler
            if not issuer_meaningful or not comp_meaningful:
                continue
            common = issuer_meaningful & comp_meaningful
            if not common:
                continue
            # Require high bidirectional overlap (>=0.8 on BOTH sides) to avoid
            # wrong-entity matches like "Southern Company" -> "Norfolk Southern"
            issuer_overlap = len(common) / len(issuer_meaningful)
            cand_overlap = len(common) / len(comp_meaningful)
            if issuer_overlap >= 0.8 and cand_overlap >= 0.8:
                logger.info(f"Skipping {issuer_name} - already has {rev_count} reviews as '{comp_name}' (overlap i={issuer_overlap:.2f} c={cand_overlap:.2f})")
                self._update_queue_status(q_id, 'completed',
                                          glassdoor_name=comp_name,
                                          reviews_extracted=rev_count,
                                          match_confidence='existing',
                                          completed_at=datetime.now())
                return

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

            try:
                extractor.save_company_metadata()
                logger.info(f"Saved company metadata for {glassdoor_name}")
            except Exception as e:
                logger.error(f"Error saving company metadata for {glassdoor_name}: {e}")

            self._score_company_reviews(glassdoor_name)
        else:
            self._update_queue_status(q_id, 'failed',
                                      error_message='Extraction failed - see extraction_failures table',
                                      reviews_extracted=extractor.new_reviews_saved)
            logger.error(f"Failed extraction for {issuer_name}")

    def _score_company_reviews(self, company_name, max_reviews=500):
        """Score unscored reviews for a company using culture dictionary.
        Processes at most max_reviews per call to stay within request timeouts."""
        try:
            conn = get_db_connection()
            if not conn:
                logger.error(f"No DB connection for scoring {company_name}")
                return
            cur = conn.cursor()

            cur.execute("""
                SELECT r.id, r.summary, r.pros, r.cons
                FROM reviews r
                LEFT JOIN review_culture_scores rcs ON r.id = rcs.review_id
                WHERE r.company_name = %s AND rcs.review_id IS NULL
                LIMIT %s
            """, (company_name, max_reviews))
            unscored = cur.fetchall()

            if not unscored:
                logger.info(f"No unscored reviews for {company_name}")
                cur.close()
                conn.close()
                return

            logger.info(f"Scoring {len(unscored)} reviews for {company_name}")
            scored = 0
            for review_id, summary, pros, cons in unscored:
                review_text = f"{summary or ''} {pros or ''} {cons or ''}"
                if not review_text.strip():
                    continue
                scores = score_review_with_dictionary(review_text)
                if not scores:
                    continue
                try:
                    cur.execute("SAVEPOINT score_review")
                    cur.execute("""
                        INSERT INTO review_culture_scores
                        (review_id, company_name,
                         process_results_score, job_employee_score, professional_parochial_score,
                         open_closed_score, tight_loose_score, pragmatic_normative_score,
                         agility_score, collaboration_score, customer_orientation_score,
                         diversity_score, execution_score, innovation_score, integrity_score,
                         performance_score, respect_score, scoring_method, confidence_level)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (review_id) DO NOTHING
                    """, (
                        review_id, company_name,
                        scores['hofstede']['process_results']['score'],
                        scores['hofstede']['job_employee']['score'],
                        scores['hofstede']['professional_parochial']['score'],
                        scores['hofstede']['open_closed']['score'],
                        scores['hofstede']['tight_loose']['score'],
                        scores['hofstede']['pragmatic_normative']['score'],
                        scores['mit_big_9']['agility']['score'],
                        scores['mit_big_9']['collaboration']['score'],
                        scores['mit_big_9']['customer_orientation']['score'],
                        scores['mit_big_9']['diversity']['score'],
                        scores['mit_big_9']['execution']['score'],
                        scores['mit_big_9']['innovation']['score'],
                        scores['mit_big_9']['integrity']['score'],
                        scores['mit_big_9']['performance']['score'],
                        scores['mit_big_9']['respect']['score'],
                        scores['scoring_method'],
                        'medium'
                    ))
                    cur.execute("RELEASE SAVEPOINT score_review")
                    scored += 1
                    if scored % 500 == 0:
                        conn.commit()
                except Exception as e:
                    cur.execute("ROLLBACK TO SAVEPOINT score_review")
                    logger.warning(f"Error scoring review {review_id}: {e}")

            conn.commit()
            logger.info(f"Scored {scored}/{len(unscored)} reviews for {company_name}")
            cur.close()
            conn.close()
        except Exception as e:
            logger.error(f"Error in _score_company_reviews for {company_name}: {e}")

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

    def retry_sector(self, sector, include_wrong_matches=False):
        """Reset failed/no_match records (and optionally loose existing-data matches) back to pending."""
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            # Always reset failed and no_match
            cur.execute("""
                UPDATE extraction_queue 
                SET status = 'pending', error_message = NULL,
                    glassdoor_name = NULL, glassdoor_id = NULL, glassdoor_url = NULL
                WHERE gics_sector = %s AND status IN ('failed', 'no_match')
            """, (sector,))
            updated = cur.rowcount
            if include_wrong_matches:
                # Also reset completions that were matched via the loose existing-company
                # algorithm (match_confidence='existing') so they get a fresh search.
                # These may have picked up wrong entities (e.g. Southern Co -> Norfolk Southern).
                cur.execute("""
                    UPDATE extraction_queue
                    SET status = 'pending', error_message = NULL,
                        glassdoor_name = NULL, glassdoor_id = NULL, glassdoor_url = NULL,
                        match_confidence = NULL, reviews_extracted = NULL
                    WHERE gics_sector = %s AND status = 'completed'
                      AND match_confidence = 'existing'
                """, (sector,))
                updated += cur.rowcount
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


# ─────────────────────────────────────────────────────────────────────────────
# Incremental Update Manager
# ─────────────────────────────────────────────────────────────────────────────

def _init_incremental_status_table():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS incremental_update_status (
                id INTEGER PRIMARY KEY DEFAULT 1,
                state VARCHAR(20) DEFAULT 'idle',
                started_at TIMESTAMP,
                updated_at TIMESTAMP DEFAULT NOW(),
                total_companies INTEGER DEFAULT 0,
                companies_done INTEGER DEFAULT 0,
                new_reviews_total INTEGER DEFAULT 0,
                current_company VARCHAR(255),
                last_error TEXT,
                monthly_last_triggered TIMESTAMP,
                CONSTRAINT incremental_single_row CHECK (id = 1)
            )
        """)
        # Safe migration: add column if upgrading from older schema
        cur.execute("""
            ALTER TABLE incremental_update_status
            ADD COLUMN IF NOT EXISTS monthly_last_triggered TIMESTAMP
        """)
        cur.execute("""
            INSERT INTO incremental_update_status (id, state)
            VALUES (1, 'idle') ON CONFLICT (id) DO NOTHING
        """)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error initializing incremental_update_status: {e}")


class IncrementalUpdateManager:
    """Background manager that fetches only new reviews for every company
    that already has a known Glassdoor company_id in the companies table."""

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
        _init_incremental_status_table()

    # ── DB helpers ──────────────────────────────────────────────────────────

    def _get_state(self):
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT state FROM incremental_update_status WHERE id = 1")
            row = cur.fetchone()
            cur.close()
            conn.close()
            return row[0] if row else 'idle'
        except Exception:
            return 'idle'

    def _set_state(self, state, **kwargs):
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            sets = ["state = %s", "updated_at = NOW()"]
            vals = [state]
            for k, v in kwargs.items():
                if v is None:
                    sets.append(f"{k} = NULL")
                else:
                    sets.append(f"{k} = %s")
                    vals.append(v)
            cur.execute(
                f"UPDATE incremental_update_status SET {', '.join(sets)} WHERE id = 1",
                vals
            )
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            logger.error(f"Error setting incremental state: {e}")

    # ── Public API ───────────────────────────────────────────────────────────

    def get_status(self):
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT state, started_at, updated_at, total_companies,
                       companies_done, new_reviews_total, current_company, last_error,
                       monthly_last_triggered
                FROM incremental_update_status WHERE id = 1
            """)
            row = cur.fetchone()
            cur.close()
            conn.close()
            if not row:
                return {'state': 'idle'}
            total = row[3] or 0
            done  = row[4] or 0
            updated_at = row[2]
            seconds_since_update = None
            if updated_at:
                delta = datetime.utcnow() - updated_at.replace(tzinfo=None)
                seconds_since_update = int(delta.total_seconds())
            return {
                'state':                   row[0],
                'started_at':              row[1].isoformat() if row[1] else None,
                'updated_at':              row[2].isoformat() if row[2] else None,
                'seconds_since_update':    seconds_since_update,
                'monthly_last_triggered':  row[8].isoformat() if row[8] else None,
                'total_companies':  total,
                'companies_done':   done,
                'new_reviews_total': row[5] or 0,
                'current_company':  row[6],
                'last_error':       row[7],
                'pct_done':         round(100 * done / total, 1) if total > 0 else 0,
            }
        except Exception as e:
            return {'state': 'error', 'error': str(e)}

    def start(self):
        state = self._get_state()
        if state == 'running':
            return {'status': 'already_running'}

        # Collect companies with known Glassdoor IDs that also have reviews
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT c.company_name, c.company_id,
                       c.gics_sector, c.gics_industry, c.gics_sub_industry
                FROM companies c
                WHERE c.company_id IS NOT NULL
                  AND EXISTS (
                      SELECT 1 FROM reviews r WHERE r.company_name = c.company_name
                  )
                ORDER BY c.company_name
            """)
            companies = cur.fetchall()
            cur.close()
            conn.close()
        except Exception as e:
            return {'status': 'error', 'error': str(e)}

        if not companies:
            return {'status': 'error', 'error': 'No companies with known Glassdoor IDs found'}

        self._set_state(
            'running',
            started_at=datetime.now(),
            total_companies=len(companies),
            companies_done=0,
            new_reviews_total=0,
            last_error=None,
        )

        self._thread = threading.Thread(
            target=self._run_incremental,
            args=(companies,),
            daemon=True,
        )
        self._thread.start()
        logger.info(f"Incremental update started for {len(companies)} companies")
        return {'status': 'started', 'total_companies': len(companies)}

    def stop(self):
        state = self._get_state()
        if state not in ('running', 'stopping'):
            return {'status': 'not_running'}
        self._set_state('stopping')
        logger.info("Incremental update stop requested")
        return {'status': 'stopping'}

    # ── Background worker ────────────────────────────────────────────────────

    def _run_incremental(self, companies):
        from extraction_openweb import OpenWebNinjaExtractor
        new_reviews_total = 0
        companies_done    = 0

        for row in companies:
            company_name, company_id = row[0], row[1]
            gics_sector   = row[2] if len(row) > 2 else None
            gics_industry = row[3] if len(row) > 3 else None
            gics_sub      = row[4] if len(row) > 4 else None

            if self._get_state() == 'stopping':
                self._set_state(
                    'stopped',
                    companies_done=companies_done,
                    new_reviews_total=new_reviews_total,
                    current_company=None,
                )
                logger.info("Incremental update stopped by user request")
                return

            self._set_state(
                'running',
                current_company=company_name,
                companies_done=companies_done,
                new_reviews_total=new_reviews_total,
            )

            try:
                extractor = OpenWebNinjaExtractor(
                    company_name=company_name,
                    company_id=company_id,
                    gics_sector=gics_sector,
                    gics_industry=gics_industry,
                    gics_sub_industry=gics_sub,
                    api_source='rapidapi',  # RapidAPI primary, OpenWeb Ninja fallback
                )
                new = extractor.extract_incremental()
                new_reviews_total += new
                if new > 0:
                    logger.info(f"Incremental [{company_name}]: +{new} new reviews")
            except Exception as e:
                err_msg = f"{company_name}: {str(e)[:200]}"
                logger.error(f"Incremental update error — {err_msg}")
                self._set_state('running', last_error=err_msg)

            companies_done += 1
            time.sleep(0.3)

        self._set_state(
            'completed',
            companies_done=companies_done,
            new_reviews_total=new_reviews_total,
            current_company=None,
        )
        logger.info(f"Incremental update completed: {new_reviews_total} new reviews "
                    f"across {companies_done} companies")


# ─────────────────────────────────────────────────────────────────────────────
# Monthly auto-scheduler
# ─────────────────────────────────────────────────────────────────────────────

def _monthly_trigger_check():
    """Start the incremental update if today is the 1st and it hasn't run this month."""
    try:
        now = datetime.now()
        if now.day != 1:
            return

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT monthly_last_triggered FROM incremental_update_status WHERE id = 1")
        row = cur.fetchone()
        cur.close()
        conn.close()

        last_triggered = row[0] if row and row[0] else None
        if last_triggered:
            if last_triggered.year == now.year and last_triggered.month == now.month:
                return  # already ran this month

        # Record trigger timestamp BEFORE starting to prevent double-fire on restart
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("UPDATE incremental_update_status SET monthly_last_triggered = NOW() WHERE id = 1")
        conn.commit()
        cur.close()
        conn.close()

        logger.info(f"Monthly auto-trigger: starting incremental update for "
                    f"{now.strftime('%B %Y')}")
        mgr = IncrementalUpdateManager.get_instance()
        result = mgr.start()
        logger.info(f"Monthly auto-trigger result: {result}")

    except Exception as e:
        logger.error(f"Monthly trigger check error: {e}")


def start_monthly_scheduler():
    """Launch a background thread that triggers the incremental update on the 1st of each month.

    On startup it immediately checks whether today is the 1st and the job
    hasn't run this month yet.  It then wakes up every hour to repeat the
    check, which means a Heroku dyno restart mid-day-1 will still catch the
    schedule within an hour.
    """
    # Ensure the table (and the monthly_last_triggered column) exist before
    # any scheduler logic tries to query or update them.
    _init_incremental_status_table()

    def _scheduler_loop():
        # Immediate startup check
        _monthly_trigger_check()
        # Hourly recurring check
        while True:
            time.sleep(3600)
            _monthly_trigger_check()

    t = threading.Thread(target=_scheduler_loop, daemon=True, name='monthly-scheduler')
    t.start()
    logger.info("Monthly incremental update scheduler started (checks every hour on the 1st)")
