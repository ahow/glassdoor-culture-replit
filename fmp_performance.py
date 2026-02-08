"""
Financial Modeling Prep (FMP) Performance Analysis Module
Fetches financial performance data from FMP API, caches in PostgreSQL,
and provides sector-specific peer statistics and composite scoring.
"""

import os
import json
import logging
import time
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from statistics import mean, stdev
import requests

logger = logging.getLogger(__name__)

FMP_BASE_URL = 'https://financialmodelingprep.com/stable'
CACHE_EXPIRY_DAYS = 30


def get_db_connection():
    try:
        database_url = os.environ.get('DATABASE_URL')
        if not database_url:
            return None
        if database_url.startswith('postgres://'):
            database_url = database_url.replace('postgres://', 'postgresql://', 1)
        conn = psycopg2.connect(database_url)
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None


def init_fmp_tables():
    conn = get_db_connection()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fmp_ticker_map (
                isin VARCHAR(50) PRIMARY KEY,
                ticker VARCHAR(50),
                company_name VARCHAR(255),
                exchange VARCHAR(50),
                resolved_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fmp_financial_cache (
                ticker VARCHAR(50) NOT NULL,
                data_type VARCHAR(50) NOT NULL,
                fiscal_year INTEGER NOT NULL,
                data_json JSONB,
                fetched_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (ticker, data_type, fiscal_year)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fmp_performance_metrics (
                company_name VARCHAR(255) PRIMARY KEY,
                isin VARCHAR(50),
                ticker VARCHAR(50),
                gics_sector VARCHAR(255),
                roe_latest REAL,
                roe_5y_avg REAL,
                op_margin_latest REAL,
                op_margin_5y_avg REAL,
                net_margin_latest REAL,
                revenue_growth_5y REAL,
                tsr_5y REAL,
                market_cap REAL,
                metrics_json JSONB,
                last_updated TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("FMP tables initialized")
        return True
    except Exception as e:
        logger.error(f"Error initializing FMP tables: {e}")
        conn.close()
        return False


class FMPPerformanceAnalyzer:
    def __init__(self):
        self.api_key = os.environ.get('FMP_API_KEY', '')
        self._sector_peer_stats_cache = {}
        self._sector_correlations_cache = {}

    def _fmp_request(self, endpoint, params=None):
        if not self.api_key:
            logger.error("FMP_API_KEY not configured")
            return None
        if params is None:
            params = {}
        params['apikey'] = self.api_key
        url = f"{FMP_BASE_URL}/{endpoint}"
        try:
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict) and 'Error Message' in data:
                    logger.warning(f"FMP error for {endpoint}: {data['Error Message']}")
                    return None
                return data
            else:
                logger.warning(f"FMP {endpoint} returned status {resp.status_code}")
                return None
        except Exception as e:
            logger.error(f"FMP request error for {endpoint}: {e}")
            return None

    def resolve_isin_to_ticker(self, isin: str) -> Optional[str]:
        if not isin:
            return None

        conn = get_db_connection()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("SELECT ticker FROM fmp_ticker_map WHERE isin = %s", (isin,))
                row = cur.fetchone()
                cur.close()
                conn.close()
                if row and row[0]:
                    return row[0]
            except Exception:
                try:
                    conn.close()
                except:
                    pass

        data = self._fmp_request('search-isin', {'isin': isin})
        if not data or not isinstance(data, list) or len(data) == 0:
            return None

        preferred = None
        for item in data:
            exchange = item.get('exchange', '') or ''
            symbol = item.get('symbol', '')
            name = item.get('name', '')
            if exchange in ('NYSE', 'NASDAQ', 'LSE', 'XETRA', 'Euronext', 'TSX'):
                preferred = item
                break
        if not preferred:
            preferred = data[0]

        ticker = preferred.get('symbol', '')
        if not ticker:
            return None

        conn = get_db_connection()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO fmp_ticker_map (isin, ticker, company_name, exchange)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (isin) DO UPDATE SET ticker = EXCLUDED.ticker
                """, (isin, ticker, preferred.get('name', ''), preferred.get('exchange', '')))
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                logger.warning(f"Error caching ticker map: {e}")
                try:
                    conn.close()
                except:
                    pass

        return ticker

    def _get_cached_financial(self, ticker: str, data_type: str) -> Optional[List]:
        conn = get_db_connection()
        if not conn:
            return None
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT data_json, fetched_at FROM fmp_financial_cache
                WHERE ticker = %s AND data_type = %s
                ORDER BY fiscal_year DESC
            """, (ticker, data_type))
            rows = cur.fetchall()
            cur.close()
            conn.close()
            if rows:
                cutoff = datetime.now() - timedelta(days=CACHE_EXPIRY_DAYS)
                if rows[0]['fetched_at'] > cutoff:
                    return [r['data_json'] for r in rows]
            return None
        except Exception:
            try:
                conn.close()
            except:
                pass
            return None

    def _cache_financial(self, ticker: str, data_type: str, data_list: List):
        conn = get_db_connection()
        if not conn:
            return
        try:
            cur = conn.cursor()
            for item in data_list:
                fiscal_year = item.get('fiscalYear') or item.get('calendarYear')
                if not fiscal_year:
                    date_str = item.get('date', '')
                    if date_str and len(date_str) >= 4:
                        fiscal_year = int(date_str[:4])
                    else:
                        continue
                try:
                    fiscal_year = int(fiscal_year)
                except (ValueError, TypeError):
                    continue
                cur.execute("""
                    INSERT INTO fmp_financial_cache (ticker, data_type, fiscal_year, data_json)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (ticker, data_type, fiscal_year) DO UPDATE
                    SET data_json = EXCLUDED.data_json, fetched_at = NOW()
                """, (ticker, data_type, fiscal_year, Json(item)))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            logger.warning(f"Error caching financial data: {e}")
            try:
                conn.close()
            except:
                pass

    def fetch_key_metrics(self, ticker: str) -> Optional[List]:
        cached = self._get_cached_financial(ticker, 'key_metrics')
        if cached:
            return cached
        data = self._fmp_request('key-metrics', {'symbol': ticker, 'period': 'annual', 'limit': 6})
        if data and isinstance(data, list) and len(data) > 0:
            self._cache_financial(ticker, 'key_metrics', data)
            return data
        return None

    def fetch_income_statement(self, ticker: str) -> Optional[List]:
        cached = self._get_cached_financial(ticker, 'income_statement')
        if cached:
            return cached
        data = self._fmp_request('income-statement', {'symbol': ticker, 'period': 'annual', 'limit': 6})
        if data and isinstance(data, list) and len(data) > 0:
            self._cache_financial(ticker, 'income_statement', data)
            return data
        return None

    def fetch_ratios(self, ticker: str) -> Optional[List]:
        cached = self._get_cached_financial(ticker, 'ratios')
        if cached:
            return cached
        data = self._fmp_request('ratios', {'symbol': ticker, 'period': 'annual', 'limit': 6})
        if data and isinstance(data, list) and len(data) > 0:
            self._cache_financial(ticker, 'ratios', data)
            return data
        return None

    def fetch_stock_price_history(self, ticker: str) -> Optional[List]:
        cached = self._get_cached_financial(ticker, 'price_history')
        if cached:
            return cached
        data = self._fmp_request('historical-price-eod/full', {'symbol': ticker})
        if data and isinstance(data, list) and len(data) > 0:
            yearly_prices = []
            by_year = {}
            for entry in data:
                date_str = entry.get('date', '')
                if len(date_str) >= 4:
                    year = date_str[:4]
                    if year not in by_year:
                        by_year[year] = entry
            for year in sorted(by_year.keys(), reverse=True)[:6]:
                entry = by_year[year]
                entry['fiscalYear'] = int(year)
                yearly_prices.append(entry)
            if yearly_prices:
                self._cache_financial(ticker, 'price_history', yearly_prices)
            return yearly_prices
        return None

    def calculate_tsr_5y(self, price_history: List) -> Optional[float]:
        if not price_history or len(price_history) < 2:
            return None
        try:
            sorted_prices = sorted(price_history, key=lambda x: x.get('date', ''), reverse=True)
            latest_price = sorted_prices[0].get('close')
            oldest_idx = min(5, len(sorted_prices) - 1)
            oldest_price = sorted_prices[oldest_idx].get('close')
            if latest_price and oldest_price and oldest_price > 0:
                years = oldest_idx
                if years > 0:
                    cagr = ((latest_price / oldest_price) ** (1 / years) - 1) * 100
                    return round(cagr, 2)
        except Exception as e:
            logger.warning(f"TSR calculation error: {e}")
        return None

    def get_performance_metrics(self, company_name: str, isin: str = None, ticker_hint: str = None) -> Optional[Dict]:
        conn = get_db_connection()
        if conn:
            try:
                cur = conn.cursor(cursor_factory=RealDictCursor)
                cur.execute("""
                    SELECT * FROM fmp_performance_metrics
                    WHERE company_name = %s AND last_updated > %s
                """, (company_name, datetime.now() - timedelta(days=CACHE_EXPIRY_DAYS)))
                row = cur.fetchone()
                cur.close()
                conn.close()
                if row:
                    metrics = {
                        'company': company_name,
                        'ticker': row['ticker'],
                        'gics_sector': row['gics_sector'],
                        'roe_latest': row['roe_latest'],
                        'roe_5y_avg': row['roe_5y_avg'],
                        'op_margin_latest': row['op_margin_latest'],
                        'op_margin_5y_avg': row['op_margin_5y_avg'],
                        'net_margin_latest': row['net_margin_latest'],
                        'revenue_growth_5y': row['revenue_growth_5y'],
                        'tsr_cagr_5y': row['tsr_5y'],
                        'market_cap': row['market_cap'],
                    }
                    if row['metrics_json']:
                        extra = row['metrics_json'] if isinstance(row['metrics_json'], dict) else json.loads(row['metrics_json'])
                        metrics.update(extra)
                    return metrics
            except Exception:
                try:
                    conn.close()
                except:
                    pass

        ticker = None
        if isin:
            ticker = self.resolve_isin_to_ticker(isin)
        if not ticker and ticker_hint:
            ticker = ticker_hint
        if not ticker:
            return None

        key_metrics = self.fetch_key_metrics(ticker)
        ratios = self.fetch_ratios(ticker)
        income = self.fetch_income_statement(ticker)
        prices = self.fetch_stock_price_history(ticker)

        metrics = {
            'company': company_name,
            'ticker': ticker,
            'matched_name': company_name,
        }

        if key_metrics and len(key_metrics) > 0:
            latest = key_metrics[0]
            metrics['roe_latest'] = latest.get('returnOnEquity')
            metrics['market_cap'] = latest.get('marketCap')

            roe_values = [km.get('returnOnEquity') for km in key_metrics[:5] if km.get('returnOnEquity') is not None]
            if roe_values:
                metrics['roe_5y_avg'] = round(mean(roe_values) * 100, 2) if all(abs(v) < 2 for v in roe_values) else round(mean(roe_values), 2)

        if ratios and len(ratios) > 0:
            latest_r = ratios[0]
            metrics['op_margin_latest'] = latest_r.get('operatingProfitMargin')
            metrics['net_margin_latest'] = latest_r.get('netProfitMargin')

            op_margins = [r.get('operatingProfitMargin') for r in ratios[:5] if r.get('operatingProfitMargin') is not None]
            if op_margins:
                metrics['op_margin_5y_avg'] = round(mean(op_margins), 4)

        if income and len(income) >= 2:
            revenues = [(inc.get('fiscalYear') or inc.get('calendarYear'), inc.get('revenue'))
                        for inc in income if inc.get('revenue')]
            revenues.sort(key=lambda x: str(x[0]) if x[0] else '0', reverse=True)
            if len(revenues) >= 2:
                latest_rev = revenues[0][1]
                oldest_idx = min(4, len(revenues) - 1)
                oldest_rev = revenues[oldest_idx][1]
                if oldest_rev and oldest_rev > 0 and latest_rev:
                    years = oldest_idx
                    if years > 0:
                        cagr = ((latest_rev / oldest_rev) ** (1 / years) - 1) * 100
                        metrics['revenue_growth_5y'] = round(cagr, 2)

        if prices:
            tsr = self.calculate_tsr_5y(prices)
            if tsr is not None:
                metrics['tsr_cagr_5y'] = tsr

        self._cache_performance_metrics(company_name, isin, ticker, metrics)
        return metrics if len(metrics) > 3 else None

    def _cache_performance_metrics(self, company_name: str, isin: str, ticker: str, metrics: Dict):
        conn = get_db_connection()
        if not conn:
            return
        try:
            sector = self._get_company_sector(company_name)
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO fmp_performance_metrics 
                (company_name, isin, ticker, gics_sector, roe_latest, roe_5y_avg,
                 op_margin_latest, op_margin_5y_avg, net_margin_latest,
                 revenue_growth_5y, tsr_5y, market_cap, metrics_json, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (company_name) DO UPDATE SET
                    isin = EXCLUDED.isin, ticker = EXCLUDED.ticker,
                    gics_sector = EXCLUDED.gics_sector,
                    roe_latest = EXCLUDED.roe_latest, roe_5y_avg = EXCLUDED.roe_5y_avg,
                    op_margin_latest = EXCLUDED.op_margin_latest,
                    op_margin_5y_avg = EXCLUDED.op_margin_5y_avg,
                    net_margin_latest = EXCLUDED.net_margin_latest,
                    revenue_growth_5y = EXCLUDED.revenue_growth_5y,
                    tsr_5y = EXCLUDED.tsr_5y, market_cap = EXCLUDED.market_cap,
                    metrics_json = EXCLUDED.metrics_json,
                    last_updated = NOW()
            """, (
                company_name, isin, ticker, sector,
                metrics.get('roe_latest'), metrics.get('roe_5y_avg'),
                metrics.get('op_margin_latest'), metrics.get('op_margin_5y_avg'),
                metrics.get('net_margin_latest'), metrics.get('revenue_growth_5y'),
                metrics.get('tsr_cagr_5y'), metrics.get('market_cap'),
                Json({k: v for k, v in metrics.items()
                      if k not in ('company', 'ticker', 'matched_name', 'roe_latest',
                                   'roe_5y_avg', 'op_margin_latest', 'op_margin_5y_avg',
                                   'net_margin_latest', 'revenue_growth_5y', 'tsr_cagr_5y',
                                   'market_cap', 'gics_sector')})
            ))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            logger.warning(f"Error caching performance metrics: {e}")
            try:
                conn.close()
            except:
                pass

    def _get_company_sector(self, company_name: str) -> Optional[str]:
        conn = get_db_connection()
        if not conn:
            return None
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT gics_sector FROM extraction_queue 
                WHERE glassdoor_name = %s OR issuer_name = %s
                LIMIT 1
            """, (company_name, company_name))
            row = cur.fetchone()
            cur.close()
            conn.close()
            return row[0] if row else None
        except Exception:
            try:
                conn.close()
            except:
                pass
            return None

    def get_company_info_from_queue(self, company_name: str) -> Optional[Dict]:
        conn = get_db_connection()
        if not conn:
            return None
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT issuer_name, issuer_ticker, isin, gics_sector, glassdoor_name
                FROM extraction_queue
                WHERE glassdoor_name = %s OR issuer_name = %s
                LIMIT 1
            """, (company_name, company_name))
            row = cur.fetchone()
            cur.close()
            conn.close()
            return dict(row) if row else None
        except Exception:
            try:
                conn.close()
            except:
                pass
            return None

    def get_peer_statistics(self, sector: str = None) -> Dict:
        cache_key = sector or '__all__'
        if cache_key in self._sector_peer_stats_cache:
            cached_at, stats = self._sector_peer_stats_cache[cache_key]
            if (datetime.now() - cached_at).total_seconds() < 3600:
                return stats

        conn = get_db_connection()
        if not conn:
            return self._default_peer_stats()

        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            if sector:
                cur.execute("""
                    SELECT roe_5y_avg, op_margin_5y_avg, tsr_5y, revenue_growth_5y
                    FROM fmp_performance_metrics
                    WHERE gics_sector = %s
                """, (sector,))
            else:
                cur.execute("""
                    SELECT roe_5y_avg, op_margin_5y_avg, tsr_5y, revenue_growth_5y
                    FROM fmp_performance_metrics
                """)
            rows = cur.fetchall()
            cur.close()
            conn.close()

            roe_vals = [r['roe_5y_avg'] for r in rows if r['roe_5y_avg'] is not None]
            margin_vals = [r['op_margin_5y_avg'] for r in rows if r['op_margin_5y_avg'] is not None]
            tsr_vals = [r['tsr_5y'] for r in rows if r['tsr_5y'] is not None]
            rev_growth_vals = [r['revenue_growth_5y'] for r in rows if r['revenue_growth_5y'] is not None]

            stats = {
                'roe_mean': mean(roe_vals) if roe_vals else 15,
                'roe_std': stdev(roe_vals) if len(roe_vals) > 1 else 5,
                'margin_mean': mean(margin_vals) if margin_vals else 0.30,
                'margin_std': stdev(margin_vals) if len(margin_vals) > 1 else 0.10,
                'tsr_mean': mean(tsr_vals) if tsr_vals else 10,
                'tsr_std': stdev(tsr_vals) if len(tsr_vals) > 1 else 15,
                'rev_growth_mean': mean(rev_growth_vals) if rev_growth_vals else 5,
                'rev_growth_std': stdev(rev_growth_vals) if len(rev_growth_vals) > 1 else 10,
                'sample_size': len(rows),
            }
            self._sector_peer_stats_cache[cache_key] = (datetime.now(), stats)
            return stats

        except Exception as e:
            logger.error(f"Error getting peer statistics: {e}")
            try:
                conn.close()
            except:
                pass
            return self._default_peer_stats()

    def _default_peer_stats(self) -> Dict:
        return {
            'roe_mean': 15, 'roe_std': 5,
            'margin_mean': 0.30, 'margin_std': 0.10,
            'tsr_mean': 10, 'tsr_std': 15,
            'rev_growth_mean': 5, 'rev_growth_std': 10,
            'sample_size': 0,
        }

    def calculate_composite_score(self, metrics: Dict, peer_stats: Dict) -> Optional[float]:
        if not metrics:
            return None

        score_components = []
        weights = []

        if metrics.get('roe_5y_avg') is not None:
            peer_mean = peer_stats.get('roe_mean', 15)
            peer_std = peer_stats.get('roe_std', 5)
            if peer_std > 0:
                z = (metrics['roe_5y_avg'] - peer_mean) / peer_std
                score_components.append(max(-2, min(2, z)))
                weights.append(0.30)

        if metrics.get('revenue_growth_5y') is not None:
            peer_mean = peer_stats.get('rev_growth_mean', 5)
            peer_std = peer_stats.get('rev_growth_std', 10)
            if peer_std > 0:
                z = (metrics['revenue_growth_5y'] - peer_mean) / peer_std
                score_components.append(max(-2, min(2, z)))
                weights.append(0.25)

        if metrics.get('tsr_cagr_5y') is not None:
            peer_mean = peer_stats.get('tsr_mean', 10)
            peer_std = peer_stats.get('tsr_std', 15)
            if peer_std > 0:
                z = (metrics['tsr_cagr_5y'] - peer_mean) / peer_std
                score_components.append(max(-2, min(2, z)))
                weights.append(0.25)

        if metrics.get('op_margin_5y_avg') is not None:
            peer_mean = peer_stats.get('margin_mean', 0.30)
            peer_std = peer_stats.get('margin_std', 0.10)
            if peer_std > 0:
                z = (metrics['op_margin_5y_avg'] - peer_mean) / peer_std
                score_components.append(max(-2, min(2, z)))
                weights.append(0.20)

        if not score_components:
            return None

        total_weight = sum(weights)
        if total_weight == 0:
            return None

        weighted_score = sum(s * w for s, w in zip(score_components, weights)) / total_weight
        normalized_score = 50 + (weighted_score * 25)
        return max(0, min(100, normalized_score))

    def calculate_correlation(self, culture_data: List[Dict], performance_data: List[Dict]) -> Dict:
        from scipy import stats as scipy_stats

        culture_dimensions = [
            'process_results', 'job_employee', 'professional_parochial',
            'open_closed', 'tight_loose', 'pragmatic_normative'
        ]
        mit_dimensions = [
            'agility', 'collaboration', 'customer_orientation', 'diversity',
            'execution', 'innovation', 'integrity', 'performance', 'respect'
        ]
        performance_metrics = [
            'roe_5y_avg', 'revenue_growth_5y', 'tsr_cagr_5y', 'op_margin_5y_avg', 'composite_score'
        ]

        results = {
            'hofstede': {},
            'mit': {},
            'summary': {
                'strongest_positive': [],
                'strongest_negative': [],
                'sample_size': 0
            }
        }

        company_data = {}
        for cd in culture_data:
            company_data[cd['company']] = {'culture': cd}
        for pd_item in performance_data:
            if pd_item['company'] in company_data:
                company_data[pd_item['company']]['performance'] = pd_item

        valid_companies = [c for c, d in company_data.items() if 'culture' in d and 'performance' in d]
        results['summary']['sample_size'] = len(valid_companies)

        if len(valid_companies) < 5:
            return results

        all_correlations = []

        for dim in culture_dimensions:
            results['hofstede'][dim] = {}
            for metric in performance_metrics:
                x_vals = []
                y_vals = []
                for company in valid_companies:
                    culture_val = company_data[company]['culture'].get('hofstede', {}).get(dim, {}).get('value')
                    perf_val = company_data[company]['performance'].get(metric)
                    if culture_val is not None and perf_val is not None:
                        x_vals.append(culture_val)
                        y_vals.append(perf_val)
                if len(x_vals) >= 5:
                    try:
                        corr, p_value = scipy_stats.pearsonr(x_vals, y_vals)
                        results['hofstede'][dim][metric] = {
                            'correlation': round(float(corr), 3),
                            'p_value': round(float(p_value), 4),
                            'significant': bool(p_value < 0.05),
                            'sample_size': len(x_vals)
                        }
                        all_correlations.append({
                            'framework': 'Hofstede', 'dimension': dim,
                            'metric': metric, 'correlation': float(corr),
                            'p_value': float(p_value)
                        })
                    except Exception as e:
                        logger.error(f"Correlation error for {dim}/{metric}: {e}")

        for dim in mit_dimensions:
            results['mit'][dim] = {}
            for metric in performance_metrics:
                x_vals = []
                y_vals = []
                for company in valid_companies:
                    culture_val = company_data[company]['culture'].get('mit', {}).get(dim, {}).get('value')
                    perf_val = company_data[company]['performance'].get(metric)
                    if culture_val is not None and perf_val is not None:
                        x_vals.append(culture_val)
                        y_vals.append(perf_val)
                if len(x_vals) >= 5:
                    try:
                        corr, p_value = scipy_stats.pearsonr(x_vals, y_vals)
                        results['mit'][dim][metric] = {
                            'correlation': round(float(corr), 3),
                            'p_value': round(float(p_value), 4),
                            'significant': bool(p_value < 0.05),
                            'sample_size': len(x_vals)
                        }
                        all_correlations.append({
                            'framework': 'MIT', 'dimension': dim,
                            'metric': metric, 'correlation': float(corr),
                            'p_value': float(p_value)
                        })
                    except Exception as e:
                        logger.error(f"Correlation error for {dim}/{metric}: {e}")

        if all_correlations:
            composite_corrs = [c for c in all_correlations if c['metric'] == 'composite_score']
            sorted_composite = sorted(composite_corrs, key=lambda x: x['correlation'], reverse=True)
            results['summary']['strongest_positive'] = sorted_composite[:5]
            results['summary']['strongest_negative'] = sorted_composite[-5:][::-1]

        return results

    def get_sector_list(self) -> List[str]:
        conn = get_db_connection()
        if not conn:
            return []
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT DISTINCT gics_sector FROM extraction_queue
                WHERE gics_sector IS NOT NULL AND gics_sector != ''
                ORDER BY gics_sector
            """)
            sectors = [r[0] for r in cur.fetchall()]
            cur.close()
            conn.close()
            return sectors
        except Exception:
            try:
                conn.close()
            except:
                pass
            return []

    def get_companies_in_sector(self, sector: str) -> List[str]:
        conn = get_db_connection()
        if not conn:
            return []
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT DISTINCT r.company_name
                FROM reviews r
                JOIN extraction_queue eq ON (eq.glassdoor_name = r.company_name)
                WHERE eq.gics_sector = %s
                ORDER BY r.company_name
            """, (sector,))
            companies = [r[0] for r in cur.fetchall()]
            cur.close()
            conn.close()
            return companies
        except Exception:
            try:
                conn.close()
            except:
                pass
            return []

    def get_company_sector(self, company_name: str) -> Optional[str]:
        return self._get_company_sector(company_name)

    def get_business_model(self, company_name: str) -> str:
        sector = self._get_company_sector(company_name)
        return sector or 'Unknown'


fmp_analyzer = FMPPerformanceAnalyzer()
