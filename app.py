"""
ACWI Glassdoor Dashboard - Production Flask Application
Simplified approach: Query database on-demand instead of pre-loading all data
"""

import os
import re
import json
import logging
import math
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, render_template, jsonify, request, Response, send_file
from datetime import datetime, timedelta
from statistics import mean
from culture_scoring import score_review_with_dictionary
from performance_analysis import performance_analyzer
from fmp_performance import fmp_analyzer, init_fmp_tables

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__, template_folder='templates', static_folder='static')

# ============================================================================
# CONFIGURATION
# ============================================================================

# Confidence scoring parameters
MIN_REVIEWS_FOR_HIGH_CONFIDENCE = 50
MIN_REVIEWS_FOR_MEDIUM_CONFIDENCE = 20

# Dimension keys
HOFSTEDE_DIMENSIONS = [
    'process_results',
    'job_employee',
    'professional_parochial',
    'open_closed',
    'tight_loose',
    'pragmatic_normative'
]

MIT_DIMENSIONS = [
    'agility',
    'collaboration',
    'customer_orientation',
    'diversity',
    'execution',
    'innovation',
    'integrity',
    'performance',
    'respect'
]

# ============================================================================
# DATABASE CONNECTION
# ============================================================================

def get_db_connection():
    """Get PostgreSQL database connection"""
    try:
        database_url = os.environ.get('DATABASE_URL')
        if not database_url:
            logger.error("DATABASE_URL environment variable not set")
            return None
        
        # Handle postgres:// vs postgresql://
        if database_url.startswith('postgres://'):
            database_url = database_url.replace('postgres://', 'postgresql://', 1)
        
        conn = psycopg2.connect(database_url)
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def calculate_relative_confidence(metrics):
    """
    Calculate relative confidence scores for each dimension.
    The dimension with the highest evidence gets 100, others scaled proportionally.
    """
    # Find max evidence across all dimensions
    max_evidence = 0
    
    # Check Hofstede dimensions
    for dimension, data in metrics.get('hofstede', {}).items():
        evidence = data.get('total_evidence', 0)
        if evidence > max_evidence:
            max_evidence = evidence
    
    # Check MIT dimensions
    for dimension, data in metrics.get('mit_big_9', {}).items():
        evidence = data.get('total_evidence', 0)
        if evidence > max_evidence:
            max_evidence = evidence
    
    # If no evidence found, use review count as fallback
    if max_evidence == 0:
        # Fallback: use review count to estimate confidence
        review_count = metrics.get('total_reviews', 0)
        if review_count > 0:
            # Estimate evidence from review count (assume avg 5 keywords per dimension per review)
            max_evidence = review_count * 5
        else:
            # No data at all, return as-is
            return metrics
    
    # Calculate relative confidence for Hofstede
    for dimension, data in metrics.get('hofstede', {}).items():
        evidence = data.get('total_evidence', 0)
        # If total_evidence is 0 but we have a value, estimate from review count
        if evidence == 0 and data.get('value') is not None:
            # Estimate: assume 3 keywords per dimension per review on average
            review_count = metrics.get('total_reviews', 0)
            evidence = max(1, review_count // 15)  # Conservative estimate
        data['confidence_score'] = round((evidence / max_evidence) * 100, 1) if max_evidence > 0 else 0
    
    # Calculate relative confidence for MIT
    for dimension, data in metrics.get('mit_big_9', {}).items():
        evidence = data.get('total_evidence', 0)
        # If total_evidence is 0 but we have a value, estimate from review count
        if evidence == 0 and data.get('value') is not None:
            # Estimate: assume 3 keywords per dimension per review on average
            review_count = metrics.get('total_reviews', 0)
            evidence = max(1, review_count // 15)  # Conservative estimate
        data['confidence_score'] = round((evidence / max_evidence) * 100, 1) if max_evidence > 0 else 0
    
    return metrics

# Cache for MIT max values (calculated once and reused)
_mit_max_values_cache = {}
_mit_max_values_by_sector = {}


_company_sector_map = {}
_company_gics_map = {}
_company_sector_map_loaded = False

UNLISTED_ASSET_MANAGERS = {
    'AllianceBernstein': 'Asset Management',
    'Capital Group': 'Asset Management',
    'Dimensional Fund Advisors': 'Asset Management',
    'Eurazeo': 'Asset Management',
    'Federated Hermes': 'Asset Management',
    'Fidelity International': 'Asset Management',
    'Fidelity Investments': 'Asset Management',
    'Franklin Templeton': 'Asset Management',
    'Invesco': 'Asset Management',
    'Natixis Investment Managers': 'Asset Management',
    'Nuveen': 'Asset Management',
    'PIMCO': 'Asset Management',
    'Robeco': 'Asset Management',
    'Vanguard Group': 'Asset Management',
    'Wellington Management': 'Asset Management',
}

def _build_company_sector_map():
    """Build a mapping from review company names to GICS sectors/industries/sub-industries using fuzzy matching."""
    global _company_sector_map, _company_gics_map, _company_sector_map_loaded
    conn = get_db_connection()
    if not conn:
        return
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT company_name FROM reviews")
        review_companies = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("""
            SELECT glassdoor_name, issuer_name, gics_sector, gics_industry, gics_sub_industry 
            FROM extraction_queue WHERE gics_sector IS NOT NULL
        """)
        eq_rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        eq_lookup = {}
        for gd_name, issuer_name, sector, industry, sub_industry in eq_rows:
            gics_info = {
                'sector': sector,
                'industry': industry or '',
                'sub_industry': sub_industry or ''
            }
            if gd_name and gd_name.strip():
                eq_lookup[gd_name.strip().lower()] = gics_info
            if issuer_name and issuer_name.strip():
                eq_lookup[issuer_name.strip().lower()] = gics_info
        
        ambiguous_words = {'capital', 'state', 'national', 'fidelity', 'hdfc', 'bank', 'general', 'international'}
        
        def assign_company(company, gics_info):
            _company_sector_map[company] = gics_info['sector']
            _company_gics_map[company] = gics_info
        
        for company in review_companies:
            if company in UNLISTED_ASSET_MANAGERS:
                _company_sector_map[company] = UNLISTED_ASSET_MANAGERS[company]
                _company_gics_map[company] = {
                    'sector': 'Asset Management',
                    'industry': 'Asset Management',
                    'sub_industry': 'Asset Management'
                }
                continue
            
            cn_lower = company.lower().strip()
            if cn_lower in eq_lookup:
                assign_company(company, eq_lookup[cn_lower])
                continue
            
            matched = False
            for eq_name, gics_info in eq_lookup.items():
                if len(eq_name) > 3 and len(cn_lower) > 3:
                    if cn_lower in eq_name or eq_name in cn_lower:
                        cn_first = cn_lower.split()[0] if cn_lower.split() else ''
                        if cn_first not in ambiguous_words:
                            assign_company(company, gics_info)
                            matched = True
                            break
            
            if not matched:
                cn_words = cn_lower.replace(',', '').replace('.', '').replace('&', '').split()
                if cn_words:
                    primary = cn_words[0]
                    if len(primary) > 3 and primary not in ambiguous_words:
                        for eq_name, gics_info in eq_lookup.items():
                            eq_clean = eq_name.replace(',', '').replace('.', '').replace('&', '')
                            if primary in eq_clean.split():
                                assign_company(company, gics_info)
                                break
        
        unmatched = [c for c in review_companies if c not in _company_sector_map]
        if unmatched:
            logger.info(f"Unmatched companies (no sector assigned): {unmatched}")
        
        _company_sector_map_loaded = True
        logger.info(f"Company-sector map built: {len(_company_sector_map)}/{len(review_companies)} companies matched")
    except Exception as e:
        logger.error(f"Error building company-sector map: {e}")
        try:
            conn.close()
        except:
            pass

AM_GICS_SUB_INDUSTRIES = {'Asset Management & Custody Banks'}

def _is_asset_management_company(company):
    """Return True if this company belongs to the Asset Management group.
    
    Covers both the 14 hardcoded unlisted firms AND listed companies whose
    GICS sub-industry is 'Asset Management & Custody Banks'.
    """
    if _company_sector_map.get(company) == 'Asset Management':
        return True
    gics = _company_gics_map.get(company, {})
    return gics.get('sub_industry') in AM_GICS_SUB_INDUSTRIES


def get_companies_for_sector(sector=None, gics_level='sector', gics_value=None):
    """Get list of company names that have reviews, optionally filtered by GICS level.
    
    Args:
        sector: Legacy parameter - GICS sector name (for backward compatibility)
        gics_level: 'sector', 'industry', or 'sub_industry'
        gics_value: The value to filter by at the specified level
    """
    global _company_sector_map_loaded
    if not _company_sector_map_loaded:
        _build_company_sector_map()
    
    filter_value = gics_value or sector
    
    conn = get_db_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT company_name FROM reviews ORDER BY company_name")
        all_companies = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        
        if filter_value:
            if filter_value == 'Asset Management':
                return [c for c in all_companies if _is_asset_management_company(c)]
            elif gics_level == 'industry':
                return [c for c in all_companies if _company_gics_map.get(c, {}).get('industry') == filter_value]
            elif gics_level == 'sub_industry':
                return [c for c in all_companies if _company_gics_map.get(c, {}).get('sub_industry') == filter_value]
            else:
                return [c for c in all_companies if _company_sector_map.get(c) == filter_value]
        return all_companies
    except Exception as e:
        logger.error(f"Error getting companies for sector: {e}")
        try:
            conn.close()
        except:
            pass
        return []


def get_company_sector(company_name):
    """Look up GICS sector for a company using cached map."""
    global _company_sector_map_loaded
    if not _company_sector_map_loaded:
        _build_company_sector_map()
    return _company_sector_map.get(company_name)


def get_company_gics(company_name):
    """Look up full GICS info (sector, industry, sub_industry) for a company."""
    global _company_sector_map_loaded
    if not _company_sector_map_loaded:
        _build_company_sector_map()
    return _company_gics_map.get(company_name, {})

def get_mit_max_values(company_names=None):
    """Get maximum MIT values for rescaling.

    When company_names is provided the max is computed only within those
    companies (sector / industry / sub-industry relative normalisation).
    When omitted the global maximum across every company is used.
    """
    global _mit_max_values_cache, _mit_max_values_by_sector

    # Choose the right cache bucket
    if company_names:
        cache_key = frozenset(company_names)
        if cache_key in _mit_max_values_by_sector:
            return _mit_max_values_by_sector[cache_key]
    else:
        if _mit_max_values_cache:
            return _mit_max_values_cache

    try:
        conn = get_db_connection()
        if not conn:
            return {dim: 1 for dim in MIT_DIMENSIONS}  # Fallback

        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Get max company-level average values for each MIT dimension
        # This calculates AVG per company first, then takes MAX of those averages
        if company_names:
            placeholders = ','.join(['%s'] * len(company_names))
            where_clause = f"WHERE company_name IN ({placeholders})"
            params = list(company_names)
        else:
            where_clause = ''
            params = []

        cursor.execute(f"""
            SELECT 
                MAX(company_avg.agility) as agility,
                MAX(company_avg.collaboration) as collaboration,
                MAX(company_avg.customer_orientation) as customer_orientation,
                MAX(company_avg.diversity) as diversity,
                MAX(company_avg.execution) as execution,
                MAX(company_avg.innovation) as innovation,
                MAX(company_avg.integrity) as integrity,
                MAX(company_avg.performance) as performance,
                MAX(company_avg.respect) as respect
            FROM (
                SELECT 
                    company_name,
                    AVG(agility_score) as agility,
                    AVG(collaboration_score) as collaboration,
                    AVG(customer_orientation_score) as customer_orientation,
                    AVG(diversity_score) as diversity,
                    AVG(execution_score) as execution,
                    AVG(innovation_score) as innovation,
                    AVG(integrity_score) as integrity,
                    AVG(performance_score) as performance,
                    AVG(respect_score) as respect
                FROM review_culture_scores
                {where_clause}
                GROUP BY company_name
            ) company_avg
        """, params)

        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if result:
            values = {
                'agility': max(float(result['agility'] or 0), 0.01),
                'collaboration': max(float(result['collaboration'] or 0), 0.01),
                'customer_orientation': max(float(result['customer_orientation'] or 0), 0.01),
                'diversity': max(float(result['diversity'] or 0), 0.01),
                'execution': max(float(result['execution'] or 0), 0.01),
                'innovation': max(float(result['innovation'] or 0), 0.01),
                'integrity': max(float(result['integrity'] or 0), 0.01),
                'performance': max(float(result['performance'] or 0), 0.01),
                'respect': max(float(result['respect'] or 0), 0.01)
            }
        else:
            values = {dim: 1 for dim in MIT_DIMENSIONS}

        # Store in the appropriate cache bucket and return
        if company_names:
            _mit_max_values_by_sector[frozenset(company_names)] = values
        else:
            _mit_max_values_cache = values

        return values

    except Exception as e:
        logger.error(f"Error getting MIT max values: {e}")
        return {dim: 1 for dim in MIT_DIMENSIONS}

def get_company_metrics(company_name):
    """Get aggregated metrics for a company from the database.
    Uses SQL aggregation instead of loading all reviews into memory."""
    try:
        conn = get_db_connection()
        if not conn:
            return None
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("""
            SELECT 
                COUNT(*) as review_count,
                AVG(rating) as avg_rating,
                AVG(work_life_balance_rating) as avg_wlb,
                AVG(culture_and_values_rating) as avg_culture,
                AVG(career_opportunities_rating) as avg_career,
                AVG(compensation_and_benefits_rating) as avg_comp,
                AVG(senior_management_rating) as avg_mgmt
            FROM reviews
            WHERE company_name = %s
        """, (company_name,))
        
        rating_result = cursor.fetchone()
        review_count = rating_result['review_count'] if rating_result else 0
        
        if review_count == 0:
            cursor.close()
            conn.close()
            return None
        
        recommend_pct = 0
        ceo_avg = 0
        try:
            cursor.execute("""
                SELECT 
                    AVG(CASE 
                        WHEN review_data->>'recommend_to_friend_rating' IS NOT NULL 
                             AND review_data->>'recommend_to_friend_rating' ~ '^[0-9.]+$'
                             AND (review_data->>'recommend_to_friend_rating')::float >= 4 
                        THEN 1.0 ELSE 0.0 END) * 100 as recommend_pct,
                    AVG(CASE 
                        WHEN review_data->>'ceo_rating' IS NOT NULL 
                             AND review_data->>'ceo_rating' ~ '^[0-9.]+$'
                        THEN (review_data->>'ceo_rating')::float 
                        ELSE NULL END) as ceo_avg
                FROM reviews
                WHERE company_name = %s 
                  AND review_data IS NOT NULL
                  AND jsonb_typeof(review_data) = 'object'
            """, (company_name,))
            rec_result = cursor.fetchone()
            if rec_result:
                recommend_pct = round(float(rec_result['recommend_pct']), 1) if rec_result['recommend_pct'] else 0
                ceo_avg = round(float(rec_result['ceo_avg']), 2) if rec_result['ceo_avg'] else 0
        except Exception as e:
            logger.warning(f"Error computing recommend/ceo for {company_name}: {e}")
            conn.rollback()
        
        cursor.execute("""
            SELECT 
                COUNT(*) as score_count,
                AVG(process_results_score) as process_results,
                AVG(job_employee_score) as job_employee,
                AVG(professional_parochial_score) as professional_parochial,
                AVG(open_closed_score) as open_closed,
                AVG(tight_loose_score) as tight_loose,
                AVG(pragmatic_normative_score) as pragmatic_normative,
                AVG(agility_score) as agility,
                AVG(collaboration_score) as collaboration,
                AVG(customer_orientation_score) as customer_orientation,
                AVG(diversity_score) as diversity,
                AVG(execution_score) as execution,
                AVG(innovation_score) as innovation,
                AVG(integrity_score) as integrity,
                AVG(performance_score) as performance,
                AVG(respect_score) as respect,
                COUNT(CASE WHEN process_results_score IS NOT NULL THEN 1 END) as process_results_count,
                COUNT(CASE WHEN job_employee_score IS NOT NULL THEN 1 END) as job_employee_count,
                COUNT(CASE WHEN professional_parochial_score IS NOT NULL THEN 1 END) as professional_parochial_count,
                COUNT(CASE WHEN open_closed_score IS NOT NULL THEN 1 END) as open_closed_count,
                COUNT(CASE WHEN tight_loose_score IS NOT NULL THEN 1 END) as tight_loose_count,
                COUNT(CASE WHEN pragmatic_normative_score IS NOT NULL THEN 1 END) as pragmatic_normative_count,
                COUNT(CASE WHEN agility_score IS NOT NULL AND agility_score > 0 THEN 1 END) as agility_count,
                COUNT(CASE WHEN collaboration_score IS NOT NULL AND collaboration_score > 0 THEN 1 END) as collaboration_count,
                COUNT(CASE WHEN customer_orientation_score IS NOT NULL AND customer_orientation_score > 0 THEN 1 END) as customer_orientation_count,
                COUNT(CASE WHEN diversity_score IS NOT NULL AND diversity_score > 0 THEN 1 END) as diversity_count,
                COUNT(CASE WHEN execution_score IS NOT NULL AND execution_score > 0 THEN 1 END) as execution_count,
                COUNT(CASE WHEN innovation_score IS NOT NULL AND innovation_score > 0 THEN 1 END) as innovation_count,
                COUNT(CASE WHEN integrity_score IS NOT NULL AND integrity_score > 0 THEN 1 END) as integrity_count,
                COUNT(CASE WHEN performance_score IS NOT NULL AND performance_score > 0 THEN 1 END) as performance_count,
                COUNT(CASE WHEN respect_score IS NOT NULL AND respect_score > 0 THEN 1 END) as respect_count
            FROM review_culture_scores
            WHERE company_name = %s
        """, (company_name,))
        
        culture_result = cursor.fetchone()
        scored_review_count = culture_result['score_count'] if culture_result else 0
        
        hofstede_dim_map = {
            'process_results': 'process_results',
            'job_employee': 'job_employee', 
            'professional_parochial': 'professional_parochial',
            'open_closed': 'open_closed',
            'tight_loose': 'tight_loose',
            'pragmatic_normative': 'pragmatic_normative'
        }
        
        mit_dim_map = {
            'agility': 'agility',
            'collaboration': 'collaboration',
            'customer_orientation': 'customer_orientation',
            'diversity': 'diversity',
            'execution': 'execution',
            'innovation': 'innovation',
            'integrity': 'integrity',
            'performance': 'performance',
            'respect': 'respect'
        }
        
        hofstede_avg = {}
        if culture_result and scored_review_count > 0:
            for db_col, dim in hofstede_dim_map.items():
                value = culture_result.get(db_col)
                count = culture_result.get(f'{db_col}_count', 0)
                if value is not None and count > 0:
                    hofstede_avg[dim] = {
                        'value': round(float(value), 2),
                        'confidence': 0,
                        'confidence_level': 'High' if count >= MIN_REVIEWS_FOR_HIGH_CONFIDENCE else 'Medium' if count >= MIN_REVIEWS_FOR_MEDIUM_CONFIDENCE else 'Low',
                        'total_evidence': count
                    }
                else:
                    hofstede_avg[dim] = {'value': 0, 'confidence': 0, 'confidence_level': 'Low', 'total_evidence': 0}
        else:
            for dim in HOFSTEDE_DIMENSIONS:
                hofstede_avg[dim] = {'value': 0, 'confidence': 0, 'confidence_level': 'Low', 'total_evidence': 0}
        
        mit_avg = {}
        if culture_result and scored_review_count > 0:
            for db_col, dim in mit_dim_map.items():
                value = culture_result.get(db_col)
                count = culture_result.get(f'{db_col}_count', 0)
                if value is not None and count > 0:
                    mit_avg[dim] = {
                        'value': round(float(value), 4),
                        'confidence': 0,
                        'confidence_level': 'High' if count >= MIN_REVIEWS_FOR_HIGH_CONFIDENCE else 'Medium' if count >= MIN_REVIEWS_FOR_MEDIUM_CONFIDENCE else 'Low',
                        'total_evidence': count
                    }
                else:
                    mit_avg[dim] = {'value': 0, 'confidence': 0, 'confidence_level': 'Low', 'total_evidence': 0}
        else:
            for dim in MIT_DIMENSIONS:
                mit_avg[dim] = {'value': 0, 'confidence': 0, 'confidence_level': 'Low', 'total_evidence': 0}
        
        metrics = {
            'company_name': company_name,
            'total_reviews': review_count,
            'overall_rating': round(float(rating_result['avg_rating']), 2) if rating_result['avg_rating'] else 0,
            'culture_values': round(float(rating_result['avg_culture']), 2) if rating_result['avg_culture'] else 0,
            'work_life_balance': round(float(rating_result['avg_wlb']), 2) if rating_result['avg_wlb'] else 0,
            'career_opportunities': round(float(rating_result['avg_career']), 2) if rating_result['avg_career'] else 0,
            'compensation_benefits': round(float(rating_result['avg_comp']), 2) if rating_result['avg_comp'] else 0,
            'senior_management': round(float(rating_result['avg_mgmt']), 2) if rating_result['avg_mgmt'] else 0,
            'recommend_percentage': recommend_pct,
            'ceo_approval': ceo_avg,
            'hofstede': hofstede_avg,
            'mit_big_9': mit_avg
        }
        
        metrics = calculate_relative_confidence(metrics)
        
        logger.info(f"Metrics for {company_name}: {review_count} reviews (SQL aggregated)")
        
        cursor.close()
        conn.close()
        return metrics
        
    except Exception as e:
        logger.error(f"Error getting company metrics: {e}")
        import traceback
        traceback.print_exc()
        return None

# ============================================================================
# CACHE MANAGEMENT
# ============================================================================

def init_cache_table():
    """Initialize the cache table if it doesn't exist"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS company_metrics_cache (
                company_name VARCHAR(255) PRIMARY KEY,
                metrics_json JSONB,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                review_count INT
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Cache table initialized")
        return True
    except Exception as e:
        logger.error(f"Error initializing cache table: {e}")
        return False


def init_extraction_queue():
    """Initialize extraction_queue table and populate from CSV if empty"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS extraction_queue (
                id SERIAL PRIMARY KEY,
                issuer_id VARCHAR(255),
                issuer_name VARCHAR(255),
                issuer_ticker VARCHAR(50),
                isin VARCHAR(50),
                country VARCHAR(10),
                gics_sector VARCHAR(255),
                gics_industry VARCHAR(255),
                gics_sub_industry VARCHAR(255),
                glassdoor_name VARCHAR(255),
                glassdoor_id INTEGER,
                glassdoor_url TEXT,
                status VARCHAR(50) DEFAULT 'pending',
                reviews_extracted INTEGER DEFAULT 0,
                review_count_glassdoor INTEGER,
                search_results JSONB,
                match_confidence VARCHAR(50),
                error_message TEXT,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.commit()

        cursor.execute("DELETE FROM extraction_queue a USING extraction_queue b WHERE a.id > b.id AND a.issuer_name = b.issuer_name")
        deleted = cursor.rowcount
        if deleted > 0:
            logger.info(f"Removed {deleted} duplicate entries from extraction_queue")
        conn.commit()

        cursor.execute("""
            UPDATE extraction_queue 
            SET status = 'pending', error_message = NULL, search_results = NULL, match_confidence = NULL
            WHERE status = 'no_match' AND error_message = 'Search returned result without company ID'
        """)
        reset_count = cursor.rowcount
        if reset_count > 0:
            logger.info(f"Reset {reset_count} companies that failed due to old field-name bug")
        conn.commit()

        cursor.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint WHERE conname = 'uq_extraction_queue_issuer_name'
                ) THEN
                    ALTER TABLE extraction_queue ADD CONSTRAINT uq_extraction_queue_issuer_name UNIQUE (issuer_name);
                END IF;
            END $$;
        """)
        conn.commit()

        cursor.execute("SELECT COUNT(*) FROM extraction_queue")
        count = cursor.fetchone()[0]

        if count == 0:
            import csv as csv_mod
            csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    'attached_assets',
                                    'Screen_Results_-_20260202_11_53_47_1770033261707.csv')
            if os.path.exists(csv_path):
                with open(csv_path, 'r', encoding='utf-8-sig') as f:
                    reader = csv_mod.DictReader(f)
                    rows_inserted = 0
                    for row in reader:
                        cursor.execute("""
                            INSERT INTO extraction_queue 
                            (issuer_id, issuer_name, issuer_ticker, isin, country,
                             gics_sector, gics_industry, gics_sub_industry)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (issuer_name) DO NOTHING
                        """, (
                            row.get('ISSUERID', ''),
                            row.get('ISSUER_NAME', ''),
                            row.get('ISSUER_TICKER', ''),
                            row.get('ISSUER_ISIN', ''),
                            row.get('ISSUER_CNTRY_DOMICILE', ''),
                            row.get('GICS_SECTOR', ''),
                            row.get('GICS_IND', ''),
                            row.get('GICS_SUB_IND', '')
                        ))
                        rows_inserted += 1
                    conn.commit()
                    logger.info(f"Loaded {rows_inserted} companies into extraction_queue from CSV")
            else:
                logger.warning(f"CSV file not found at {csv_path}")
        else:
            logger.info(f"Extraction queue already has {count} companies")

        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error initializing extraction queue: {e}")
        return False

def get_cached_metrics_batch(company_names):
    """Get metrics from cache for multiple companies in a single query"""
    if not company_names:
        return {}
    try:
        conn = get_db_connection()
        if not conn:
            return {}
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        placeholders = ','.join(['%s'] * len(company_names))
        cursor.execute(f"""
            SELECT company_name, metrics_json FROM company_metrics_cache
            WHERE company_name IN ({placeholders})
        """, list(company_names))
        result = {}
        for row in cursor.fetchall():
            m = row['metrics_json'] if isinstance(row['metrics_json'], dict) else json.loads(row['metrics_json'])
            result[row['company_name']] = m
        cursor.close()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Error getting batch cached metrics: {e}")
        return {}


def get_cached_metrics(company_name):
    """Get metrics from cache if available"""
    try:
        conn = get_db_connection()
        if not conn:
            return None
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT metrics_json, last_updated, review_count 
            FROM company_metrics_cache 
            WHERE company_name = %s
        """, (company_name,))
        
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result:
            metrics = result['metrics_json'] if isinstance(result['metrics_json'], dict) else json.loads(result['metrics_json'])
            return metrics
        return None
    except Exception as e:
        logger.error(f"Error getting cached metrics: {e}")
        return None

def cache_metrics(company_name, metrics):
    """Store metrics in cache"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        metrics_json = json.dumps(metrics) if not isinstance(metrics, str) else metrics
        review_count = metrics.get('total_reviews', 0)
        
        cursor.execute("""
            INSERT INTO company_metrics_cache (company_name, metrics_json, review_count, last_updated)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (company_name) DO UPDATE SET
                metrics_json = EXCLUDED.metrics_json,
                review_count = EXCLUDED.review_count,
                last_updated = CURRENT_TIMESTAMP
        """, (company_name, metrics_json, review_count))
        
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Cached metrics for {company_name}")
        return True
    except Exception as e:
        logger.error(f"Error caching metrics: {e}")
        return False

def invalidate_cache(company_name=None):
    """Invalidate cache for a company or all companies"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        if company_name:
            cursor.execute("DELETE FROM company_metrics_cache WHERE company_name = %s", (company_name,))
            logger.info(f"Invalidated cache for {company_name}")
        else:
            cursor.execute("DELETE FROM company_metrics_cache")
            logger.info("Invalidated all cache")
        
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error invalidating cache: {e}")
        return False

# ============================================================================
# ROUTES
# ============================================================================

@app.route('/', methods=['GET'])
def index():
    """Serve the main dashboard"""
    return render_template('index.html')


@app.route('/api/warm-cache', methods=['POST'])
def warm_cache():
    """Warm the metrics cache for uncached companies (batch of 20 at a time)"""
    try:
        company_names = get_companies_for_sector(None)
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        cursor = conn.cursor()
        cursor.execute("SELECT company_name FROM company_metrics_cache")
        cached = set(row[0] for row in cursor.fetchall())
        cursor.close()
        conn.close()
        
        uncached = [c for c in company_names if c not in cached]
        batch_size = 20
        warmed = 0
        for company_name in uncached[:batch_size]:
            metrics = get_company_metrics(company_name)
            if metrics:
                cache_metrics(company_name, metrics)
                warmed += 1
        
        return jsonify({
            'success': True,
            'warmed': warmed,
            'remaining': max(0, len(uncached) - batch_size),
            'total_cached': len(cached) + warmed,
            'total_companies': len(company_names)
        })
    except Exception as e:
        logger.error(f"Error warming cache: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/score-reviews', methods=['POST'])
def score_unscored_reviews():
    """Score unscored reviews for companies that have reviews but no culture scores.
    Processes one company per call, capped at 500 reviews, to stay within request timeouts.
    The frontend loop calls this repeatedly until remaining == 0."""
    try:
        # Cap per-call review limit to avoid Gunicorn worker timeout
        max_reviews_per_call = int(request.args.get('max_reviews', 500))
        max_reviews_per_call = min(max_reviews_per_call, 500)

        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500

        cursor = conn.cursor()
        # Pick the company with the fewest unscored reviews first (fastest to complete)
        cursor.execute("""
            SELECT r.company_name, COUNT(r.id) as unscored_count
            FROM reviews r
            LEFT JOIN review_culture_scores rcs ON r.id = rcs.review_id
            WHERE rcs.review_id IS NULL
            GROUP BY r.company_name
            ORDER BY COUNT(r.id) ASC
            LIMIT 1
        """)
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if not row:
            return jsonify({
                'success': True,
                'message': 'All reviews are scored',
                'scored_companies': 0,
                'remaining': 0,
                'remaining_reviews': 0
            })

        company_name, unscored_count = row

        from extraction_manager import ExtractionManager
        mgr = ExtractionManager.get_instance()

        try:
            mgr._score_company_reviews(company_name, max_reviews=max_reviews_per_call)
            invalidate_cache(company_name)
            _mit_max_values_cache.clear()
            status = 'scored'
        except Exception as e:
            logger.error(f"Error scoring {company_name}: {e}")
            status = f'error: {str(e)}'

        # Count remaining companies AND reviews after this batch
        conn = get_db_connection()
        remaining_companies = 0
        remaining_reviews = 0
        if conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(DISTINCT r.company_name), COUNT(r.id)
                FROM reviews r
                LEFT JOIN review_culture_scores rcs ON r.id = rcs.review_id
                WHERE rcs.review_id IS NULL
            """)
            row2 = cursor.fetchone()
            if row2:
                remaining_companies, remaining_reviews = row2
            cursor.close()
            conn.close()

        return jsonify({
            'success': True,
            'scored_companies': 1,
            'remaining': remaining_companies,
            'remaining_reviews': remaining_reviews,
            'results': [{'company': company_name, 'unscored_reviews': unscored_count, 'status': status}]
        })
    except Exception as e:
        logger.error(f"Error scoring reviews: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/data-status', methods=['GET'])
def data_status():
    """Return extraction progress for reviews, culture scoring, and FMP data."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SET statement_timeout = 20000")  # 20-second cap per query

        def fmt(dt):
            return dt.strftime('%d %b %Y') if dt else None

        # 1. All companies in queue with sector + ISIN info (fast: 2442 rows)
        cur.execute("SELECT glassdoor_name, gics_sector, isin FROM extraction_queue")
        queue_rows = cur.fetchall()
        total_in_queue = len(queue_rows)
        queue_map = {r['glassdoor_name']: r for r in queue_rows}
        queue_names = set(queue_map.keys())

        # 2. Companies that have reviews — aggregate per company (uses idx on company_name)
        cur.execute("""
            SELECT company_name,
                   COUNT(*) AS review_count,
                   MIN(created_at) AS earliest_extracted,
                   MAX(created_at) AS latest_extracted,
                   MIN(review_datetime) AS earliest_review,
                   MAX(review_datetime) AS latest_review
            FROM reviews
            GROUP BY company_name
        """)
        rev_rows = {r['company_name']: r for r in cur.fetchall()}
        # Filter to only companies in extraction_queue
        rev_in_queue = {k: v for k, v in rev_rows.items() if k in queue_names}

        # 3. Companies that have culture scores (fast: only company_name column)
        cur.execute("""
            SELECT company_name,
                   MIN(created_at) AS earliest_scored,
                   MAX(created_at) AS latest_scored
            FROM review_culture_scores
            GROUP BY company_name
        """)
        cult_rows = {r['company_name']: r for r in cur.fetchall() if r['company_name'] in queue_names}

        # 4. FMP table — small, fast
        cur.execute("""
            SELECT company_name, data_source, roe_5y_avg, roe_latest,
                   op_margin_5y_avg, tsr_5y, last_updated
            FROM fmp_performance_metrics
        """)
        fmp_rows = {r['company_name']: r for r in cur.fetchall()}

        cur.close()
        conn.close()

        # --- Aggregate reviews overall ---
        companies_with_reviews = len(rev_in_queue)
        total_reviews = sum(r['review_count'] for r in rev_in_queue.values())
        rev_earliest_ext = min((r['earliest_extracted'] for r in rev_in_queue.values() if r['earliest_extracted']), default=None)
        rev_latest_ext = max((r['latest_extracted'] for r in rev_in_queue.values() if r['latest_extracted']), default=None)
        rev_earliest_rev = min((r['earliest_review'] for r in rev_in_queue.values() if r['earliest_review']), default=None)
        rev_latest_rev = max((r['latest_review'] for r in rev_in_queue.values() if r['latest_review']), default=None)

        # --- Aggregate culture overall ---
        companies_scored = len(cult_rows)
        cult_earliest = min((r['earliest_scored'] for r in cult_rows.values() if r['earliest_scored']), default=None)
        cult_latest = max((r['latest_scored'] for r in cult_rows.values() if r['latest_scored']), default=None)

        # --- Aggregate FMP overall (only companies with reviews AND ISIN) ---
        def _has_real_fmp(fpm):
            if fpm is None: return False
            if fpm['data_source'] == 'no_data': return False
            return (fpm['roe_5y_avg'] is not None or fpm['roe_latest'] is not None
                    or fpm['op_margin_5y_avg'] is not None or fpm['tsr_5y'] is not None
                    or fpm['data_source'] == 'excel')

        fmp_eligible = set()
        fmp_with_data = set()
        fmp_no_data = set()
        fmp_dates = []
        for name, qrow in queue_map.items():
            if not qrow['isin'] or not qrow['isin'].strip():
                continue
            if name not in rev_in_queue:
                continue
            fmp_eligible.add(name)
            fpm = fmp_rows.get(name)
            if fpm:
                if fpm['data_source'] == 'no_data':
                    fmp_no_data.add(name)
                elif _has_real_fmp(fpm):
                    fmp_with_data.add(name)
                    if fpm['last_updated']:
                        fmp_dates.append(fpm['last_updated'])

        eligible = len(fmp_eligible)
        with_data = len(fmp_with_data)
        fmp_earliest = min(fmp_dates, default=None)
        fmp_latest = max(fmp_dates, default=None)

        # --- Per-sector breakdown (pure Python aggregation) ---
        sector_agg = {}
        for name, qrow in queue_map.items():
            sector = qrow['gics_sector'] or '(Unknown)'
            isin = qrow['isin'] or ''
            if sector not in sector_agg:
                sector_agg[sector] = {'sector': sector, 'total_in_sector': 0,
                                       'has_reviews': 0, 'has_culture_scores': 0,
                                       'has_isin': 0, 'has_fmp_data': 0}
            s = sector_agg[sector]
            s['total_in_sector'] += 1
            if name in rev_in_queue:
                s['has_reviews'] += 1
            if name in cult_rows:
                s['has_culture_scores'] += 1
            if isin.strip():
                s['has_isin'] += 1
            fpm = fmp_rows.get(name)
            if isin.strip() and _has_real_fmp(fpm):
                s['has_fmp_data'] += 1

        sector_rows_out = sorted(sector_agg.values(),
                                  key=lambda x: (-x['has_reviews'], -x['total_in_sector']))

        return jsonify({
            'success': True,
            'total_in_queue': total_in_queue,
            'reviews': {
                'companies_with_reviews': companies_with_reviews,
                'pct': round(companies_with_reviews / total_in_queue * 100, 1) if total_in_queue else 0,
                'total_reviews': total_reviews,
                'earliest_extracted': fmt(rev_earliest_ext),
                'latest_extracted': fmt(rev_latest_ext),
                'earliest_review': fmt(rev_earliest_rev),
                'latest_review': fmt(rev_latest_rev),
            },
            'culture': {
                'companies_scored': companies_scored,
                'pct': round(companies_scored / companies_with_reviews * 100, 1) if companies_with_reviews else 0,
                'earliest_scored': fmt(cult_earliest),
                'latest_scored': fmt(cult_latest),
            },
            'fmp': {
                'eligible': eligible,
                'with_data': with_data,
                'no_data': len(fmp_no_data),
                'pct': round(with_data / eligible * 100, 1) if eligible else 0,
                'earliest_updated': fmt(fmp_earliest),
                'latest_updated': fmt(fmp_latest),
            },
            'by_sector': sector_rows_out,
        })
    except Exception as e:
        logger.error(f"Error in data_status: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/perf-diag', methods=['GET'])
def perf_diagnostic():
    """Diagnostic: check why FMP fetch reports 'all done' with so few companies."""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'No DB connection'}), 500
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM extraction_queue WHERE isin IS NOT NULL AND isin != ''")
        queue_with_isin = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM extraction_queue WHERE glassdoor_name IS NOT NULL")
        queue_with_gd_name = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(DISTINCT eq.id) FROM extraction_queue eq
            INNER JOIN reviews r ON r.company_name = eq.glassdoor_name
        """)
        queue_reviews_join = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(DISTINCT eq.id) FROM extraction_queue eq
            INNER JOIN reviews r ON r.company_name = eq.glassdoor_name
            WHERE eq.isin IS NOT NULL AND eq.isin != ''
        """)
        queue_reviews_with_isin = cur.fetchone()[0]

        _has_data = """(fpm.roe_5y_avg IS NOT NULL OR fpm.op_margin_5y_avg IS NOT NULL
                       OR fpm.tsr_5y IS NOT NULL OR fpm.revenue_growth_5y IS NOT NULL
                       OR fpm.roe_latest IS NOT NULL OR fpm.op_margin_latest IS NOT NULL
                       OR fpm.data_source = 'no_data')"""
        cur.execute(f"""
            SELECT COUNT(DISTINCT eq.id) FROM extraction_queue eq
            INNER JOIN reviews r ON r.company_name = eq.glassdoor_name
            LEFT JOIN fmp_performance_metrics fpm
              ON fpm.company_name = eq.glassdoor_name AND {_has_data}
            WHERE eq.isin IS NOT NULL AND eq.isin != ''
              AND fpm.company_name IS NULL
        """)
        fmp_remaining = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM fmp_performance_metrics")
        fmp_total = cur.fetchone()[0]

        cur.execute(f"""
            SELECT eq.glassdoor_name, eq.isin, eq.gics_sector FROM extraction_queue eq
            INNER JOIN reviews r ON r.company_name = eq.glassdoor_name
            LEFT JOIN fmp_performance_metrics fpm
              ON fpm.company_name = eq.glassdoor_name AND {_has_data}
            WHERE eq.isin IS NOT NULL AND eq.isin != ''
              AND fpm.company_name IS NULL
            GROUP BY eq.glassdoor_name, eq.isin, eq.gics_sector
            LIMIT 10
        """)
        sample_missing = [{'name': r[0], 'isin': r[1], 'sector': r[2]} for r in cur.fetchall()]

        cur.execute("""
            SELECT eq.gics_sector, COUNT(DISTINCT eq.id) as cnt
            FROM extraction_queue eq
            INNER JOIN reviews r ON r.company_name = eq.glassdoor_name
            WHERE eq.isin IS NOT NULL AND eq.isin != ''
            GROUP BY eq.gics_sector ORDER BY cnt DESC
        """)
        by_sector = [{'sector': r[0], 'count': r[1]} for r in cur.fetchall()]

        # How many rows actually have financial data vs empty placeholders
        cur.execute("""
            SELECT COUNT(*) FROM fmp_performance_metrics
            WHERE roe_5y_avg IS NOT NULL OR op_margin_5y_avg IS NOT NULL
               OR tsr_5y IS NOT NULL OR revenue_growth_5y IS NOT NULL
               OR roe_latest IS NOT NULL OR op_margin_latest IS NOT NULL
        """)
        fmp_with_real_data = cur.fetchone()[0]

        # Breakdown by sector of rows with real financial data
        cur.execute("""
            SELECT gics_sector, COUNT(*) as total,
                   COUNT(CASE WHEN roe_5y_avg IS NOT NULL OR op_margin_5y_avg IS NOT NULL
                               OR tsr_5y IS NOT NULL OR revenue_growth_5y IS NOT NULL
                               OR roe_latest IS NOT NULL OR op_margin_latest IS NOT NULL
                          THEN 1 END) as has_data
            FROM fmp_performance_metrics
            GROUP BY gics_sector ORDER BY has_data DESC
        """)
        fmp_by_sector = [{'sector': r[0], 'total': r[1], 'has_data': r[2]} for r in cur.fetchall()]

        # Sample companies with actual financial data (non-NULL)
        cur.execute("""
            SELECT company_name, gics_sector,
                   COALESCE(roe_5y_avg, roe_latest) as roe,
                   COALESCE(op_margin_5y_avg, op_margin_latest) as margin,
                   tsr_5y
            FROM fmp_performance_metrics
            WHERE roe_5y_avg IS NOT NULL OR tsr_5y IS NOT NULL
               OR roe_latest IS NOT NULL OR op_margin_latest IS NOT NULL
            LIMIT 10
        """)
        sample_with_data = [{'name': r[0], 'sector': r[1], 'roe': r[2], 'margin': r[3], 'tsr': r[4]} for r in cur.fetchall()]

        cur.close()
        conn.close()
        return jsonify({
            'queue_with_isin': queue_with_isin,
            'queue_with_glassdoor_name': queue_with_gd_name,
            'queue_reviews_exact_join': queue_reviews_join,
            'queue_reviews_with_isin': queue_reviews_with_isin,
            'fmp_remaining': fmp_remaining,
            'fmp_total': fmp_total,
            'fmp_with_real_data': fmp_with_real_data,
            'fmp_by_sector': fmp_by_sector,
            'sample_with_data': sample_with_data,
            'sample_missing_fmp': sample_missing,
            'by_sector': by_sector
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/score-status', methods=['GET'])
def get_score_status():
    """Get count of companies with unscored reviews vs total."""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(DISTINCT company_name) FROM reviews
        """)
        total_companies = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(DISTINCT r.company_name)
            FROM reviews r
            LEFT JOIN review_culture_scores rcs ON r.id = rcs.review_id
            WHERE rcs.review_id IS NULL
        """)
        unscored_companies = cur.fetchone()[0]
        cur.close()
        conn.close()
        return jsonify({
            'success': True,
            'total_companies': total_companies,
            'unscored_companies': unscored_companies,
            'scored_companies': total_companies - unscored_companies
        })
    except Exception as e:
        logger.error(f"Error getting score status: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/score-company/<company_name>', methods=['POST'])
def score_single_company(company_name):
    """Score reviews for a single specific company on demand."""
    try:
        from extraction_manager import ExtractionManager
        mgr = ExtractionManager.get_instance()
        mgr._score_company_reviews(company_name)
        invalidate_cache(company_name)
        _mit_max_values_cache.clear()  # Force recalculation of normalization
        conn = get_db_connection()
        scored_count = 0
        if conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT COUNT(*) FROM review_culture_scores WHERE company_name = %s
            """, (company_name,))
            scored_count = cur.fetchone()[0]
            cur.close()
            conn.close()
        return jsonify({
            'success': True,
            'company': company_name,
            'scored_reviews': scored_count
        })
    except Exception as e:
        logger.error(f"Error scoring company {company_name}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/reset-fmp-nodata', methods=['POST'])
def reset_fmp_nodata():
    """Delete no_data markers AND partial FMP rows (TSR only, no ROE/margin) so companies are fully re-fetched."""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        cur = conn.cursor()
        cur.execute("DELETE FROM fmp_performance_metrics WHERE data_source = 'no_data'")
        deleted_nodata = cur.rowcount
        cur.execute("""
            DELETE FROM fmp_performance_metrics
            WHERE data_source = 'fmp'
              AND roe_5y_avg IS NULL AND roe_latest IS NULL
              AND op_margin_5y_avg IS NULL AND op_margin_latest IS NULL
        """)
        deleted_partial = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        total = deleted_nodata + deleted_partial
        logger.info(f"Reset FMP: deleted {deleted_nodata} no_data + {deleted_partial} partial rows")
        return jsonify({'success': True, 'deleted': total,
                        'deleted_nodata': deleted_nodata, 'deleted_partial': deleted_partial,
                        'message': f'Cleared {deleted_nodata} no-data markers and {deleted_partial} partial entries. Companies will be fully re-fetched.'})
    except Exception as e:
        logger.error(f"Error resetting FMP rows: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/fetch-fmp-performance', methods=['POST'])
def fetch_fmp_performance():
    """Batch fetch FMP financial data for companies that have ISINs but no performance data yet."""
    try:
        batch_size = int(request.args.get('batch', 5))
        batch_size = min(batch_size, 20)
        force = request.args.get('force', 'false').lower() == 'true'
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        has_data_cond = """(fpm.roe_5y_avg IS NOT NULL OR fpm.op_margin_5y_avg IS NOT NULL
                          OR fpm.tsr_5y IS NOT NULL OR fpm.revenue_growth_5y IS NOT NULL
                          OR fpm.roe_latest IS NOT NULL OR fpm.op_margin_latest IS NOT NULL
                          OR fpm.data_source = 'no_data')"""
        
        after = request.args.get('after', '')

        if force:
            if after:
                cursor.execute("""
                    SELECT eq.glassdoor_name, eq.issuer_name, eq.isin, eq.issuer_ticker,
                           eq.gics_sector, eq.gics_industry, eq.gics_sub_industry
                    FROM extraction_queue eq
                    INNER JOIN reviews r ON r.company_name = eq.glassdoor_name
                    WHERE eq.isin IS NOT NULL AND eq.isin != ''
                      AND eq.glassdoor_name > %s
                    GROUP BY eq.id, eq.glassdoor_name, eq.issuer_name, eq.isin, eq.issuer_ticker,
                             eq.gics_sector, eq.gics_industry, eq.gics_sub_industry
                    ORDER BY eq.glassdoor_name
                    LIMIT %s
                """, (after, batch_size))
            else:
                cursor.execute("""
                    SELECT eq.glassdoor_name, eq.issuer_name, eq.isin, eq.issuer_ticker,
                           eq.gics_sector, eq.gics_industry, eq.gics_sub_industry
                    FROM extraction_queue eq
                    INNER JOIN reviews r ON r.company_name = eq.glassdoor_name
                    WHERE eq.isin IS NOT NULL AND eq.isin != ''
                    GROUP BY eq.id, eq.glassdoor_name, eq.issuer_name, eq.isin, eq.issuer_ticker,
                             eq.gics_sector, eq.gics_industry, eq.gics_sub_industry
                    ORDER BY eq.glassdoor_name
                    LIMIT %s
                """, (batch_size,))
        else:
            cursor.execute(f"""
                SELECT eq.glassdoor_name, eq.issuer_name, eq.isin, eq.issuer_ticker,
                       eq.gics_sector, eq.gics_industry, eq.gics_sub_industry
                FROM extraction_queue eq
                INNER JOIN reviews r ON r.company_name = eq.glassdoor_name
                LEFT JOIN fmp_performance_metrics fpm
                  ON fpm.company_name = eq.glassdoor_name AND {has_data_cond}
                WHERE eq.isin IS NOT NULL AND eq.isin != ''
                  AND fpm.company_name IS NULL
                GROUP BY eq.id, eq.glassdoor_name, eq.issuer_name, eq.isin, eq.issuer_ticker,
                         eq.gics_sector, eq.gics_industry, eq.gics_sub_industry
                ORDER BY eq.glassdoor_name
                LIMIT %s
            """, (batch_size,))
        
        companies_to_fetch = cursor.fetchall()
        last_company = companies_to_fetch[-1]['glassdoor_name'] if companies_to_fetch else ''

        if force:
            after_clause = "AND eq.glassdoor_name > %s" if after else ""
            params = (after,) if after else ()
            cursor.execute(f"""
                SELECT COUNT(DISTINCT eq.glassdoor_name)
                FROM extraction_queue eq
                INNER JOIN reviews r ON r.company_name = eq.glassdoor_name
                WHERE eq.isin IS NOT NULL AND eq.isin != ''
                  {after_clause}
            """, params)
            total_remaining = cursor.fetchone()['count'] - len(companies_to_fetch)
        else:
            cursor.execute(f"""
                SELECT COUNT(DISTINCT eq.glassdoor_name) 
                FROM extraction_queue eq
                INNER JOIN reviews r ON r.company_name = eq.glassdoor_name
                LEFT JOIN fmp_performance_metrics fpm
                  ON fpm.company_name = eq.glassdoor_name AND {has_data_cond}
                WHERE eq.isin IS NOT NULL AND eq.isin != ''
                  AND fpm.company_name IS NULL
            """)
            total_remaining = cursor.fetchone()['count']
        
        cursor.close()
        conn.close()
        
        if not companies_to_fetch:
            return jsonify({
                'success': True,
                'message': 'All companies with ISINs have performance data',
                'fetched': 0,
                'remaining': 0,
                'last_company': ''
            })
        
        results = []
        import time as _time
        no_data_companies = []
        _batch_start = _time.time()
        _time_budget = 24  # stop before Heroku's 30-second router hard limit
        last_company = after  # cursor starts at where we left off

        for company in companies_to_fetch:
            # Abort gracefully if we're approaching the time limit
            if _time.time() - _batch_start > _time_budget:
                logger.info(f"Time budget reached after {len(results)} companies, stopping batch early")
                break

            company_name = company['glassdoor_name']
            isin = company['isin']
            ticker_hint = company.get('issuer_ticker')
            
            try:
                metrics = fmp_analyzer.get_performance_metrics(
                    company_name, isin=isin, ticker_hint=ticker_hint
                )
                if metrics:
                    results.append({
                        'company': company_name,
                        'isin': isin,
                        'ticker': metrics.get('ticker', ''),
                        'status': 'success',
                        'roe_5y_avg': metrics.get('roe_5y_avg'),
                        'tsr_5y': metrics.get('tsr_cagr_5y')
                    })
                else:
                    no_data_companies.append(company_name)
                    results.append({
                        'company': company_name,
                        'isin': isin,
                        'status': 'no_data'
                    })
            except Exception as e:
                results.append({
                    'company': company_name,
                    'isin': isin,
                    'status': f'error: {str(e)}'
                })
            
            last_company = company_name
            _time.sleep(0.2)
        
        # Mark companies with no available FMP data so they're not retried each run
        if no_data_companies:
            try:
                mark_conn = get_db_connection()
                if mark_conn:
                    mark_cur = mark_conn.cursor()
                    for nd_name in no_data_companies:
                        mark_cur.execute("""
                            INSERT INTO fmp_performance_metrics
                            (company_name, data_source, last_updated)
                            VALUES (%s, 'no_data', NOW())
                            ON CONFLICT (company_name) DO UPDATE SET
                                data_source = CASE
                                    WHEN fmp_performance_metrics.data_source = 'excel' THEN 'excel'
                                    ELSE 'no_data' END,
                                last_updated = NOW()
                        """, (nd_name,))
                    mark_conn.commit()
                    mark_cur.close()
                    mark_conn.close()
            except Exception as mark_err:
                logger.warning(f"Could not mark no_data companies: {mark_err}")

        processed = len(results)
        if force:
            # total_remaining = count_after_cursor - batch_size; add back unprocessed items
            remaining_after = max(0, total_remaining + (len(companies_to_fetch) - processed))
        else:
            remaining_after = max(0, total_remaining - processed)
        return jsonify({
            'success': True,
            'fetched': processed,
            'successful': len([r for r in results if r['status'] == 'success']),
            'no_data': len(no_data_companies),
            'remaining': remaining_after,
            'last_company': last_company,
            'results': results
        })
    except Exception as e:
        logger.error(f"Error fetching FMP performance: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


def get_gics_filter_params():
    """Extract GICS filtering parameters from request args."""
    gics_level = request.args.get('gics_level', 'sector')
    gics_value = request.args.get('gics_value') or request.args.get('sector')
    if gics_level not in ('sector', 'industry', 'sub_industry'):
        gics_level = 'sector'
    return gics_level, gics_value


@app.route('/api/sectors', methods=['GET'])
def get_sectors():
    """Get list of GICS sectors available in the data"""
    sectors = fmp_analyzer.get_sector_list()
    if 'Asset Management' not in sectors:
        sectors.append('Asset Management')
        sectors.sort()
    return jsonify({'success': True, 'sectors': sectors})


@app.route('/api/gics-hierarchy', methods=['GET'])
def get_gics_hierarchy():
    """Get full GICS hierarchy (sectors -> industries -> sub-industries) for companies with reviews."""
    global _company_sector_map_loaded
    if not _company_sector_map_loaded:
        _build_company_sector_map()
    
    hierarchy = {}
    industry_list = set()
    sub_industry_list = set()
    
    for company, gics in _company_gics_map.items():
        sector = gics.get('sector', '')
        industry = gics.get('industry', '')
        sub_industry = gics.get('sub_industry', '')
        
        if sector not in hierarchy:
            hierarchy[sector] = {}
        if industry and industry not in hierarchy[sector]:
            hierarchy[sector][industry] = set()
        if industry and sub_industry:
            hierarchy[sector][industry].add(sub_industry)
            sub_industry_list.add(sub_industry)
        if industry:
            industry_list.add(industry)
    
    hierarchy_serializable = {}
    for sector, industries in sorted(hierarchy.items()):
        hierarchy_serializable[sector] = {}
        for industry, sub_industries in sorted(industries.items()):
            hierarchy_serializable[sector][industry] = sorted(sub_industries)
    
    return jsonify({
        'success': True,
        'hierarchy': hierarchy_serializable,
        'sectors': sorted(hierarchy.keys()),
        'industry_count': len(industry_list),
        'sub_industry_count': len(sub_industry_list)
    })


@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get overall dashboard statistics, optionally filtered by sector"""
    try:
        gics_level, gics_value = get_gics_filter_params()
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        company_names = get_companies_for_sector(gics_level=gics_level, gics_value=gics_value)
        
        if company_names:
            placeholders = ','.join(['%s'] * len(company_names))
            cursor.execute(f"""
                SELECT 
                    COUNT(DISTINCT company_name) as total_companies,
                    COUNT(*) as total_reviews,
                    AVG(rating) as average_rating
                FROM reviews
                WHERE company_name IN ({placeholders})
            """, company_names)
        else:
            cursor.execute("SELECT 0 as total_companies, 0 as total_reviews, NULL as average_rating")
        
        stats = cursor.fetchone()
        
        cached_metrics_map = {}
        if company_names:
            placeholders = ','.join(['%s'] * len(company_names))
            cursor.execute(f"""
                SELECT company_name, metrics_json FROM company_metrics_cache
                WHERE company_name IN ({placeholders})
            """, company_names)
            for row in cursor.fetchall():
                m = row['metrics_json'] if isinstance(row['metrics_json'], dict) else json.loads(row['metrics_json'])
                cached_metrics_map[row['company_name']] = m
        
        cursor.close()
        conn.close()
        
        companies = []
        uncached_count = 0
        for company_name in company_names:
            metrics = cached_metrics_map.get(company_name)
            if not metrics:
                uncached_count += 1
                if uncached_count <= 50:
                    metrics = get_company_metrics(company_name)
                    if metrics:
                        cache_metrics(company_name, metrics)
                else:
                    continue
            
            if metrics:
                companies.append({
                    'id': company_name.lower().replace(' ', ''),
                    'name': company_name,
                    'total_reviews': metrics.get('total_reviews', 0),
                    'overall_rating': metrics.get('overall_rating', 0),
                    'culture_values': metrics.get('culture_values', 0),
                    'work_life_balance': metrics.get('work_life_balance', 0),
                    'career_opportunities': metrics.get('career_opportunities', 0),
                    'compensation_benefits': metrics.get('compensation_benefits', 0),
                    'senior_management': metrics.get('senior_management', 0),
                    'recommend_percentage': metrics.get('recommend_percentage', 0),
                    'ceo_approval': metrics.get('ceo_approval', 0),
                    'industry': get_company_sector(company_name) or ''
                })
        
        if uncached_count > 50:
            logger.warning(f"Stats: {uncached_count} uncached companies, loaded first 50. Use /api/warm-cache to populate rest.")
        
        return jsonify({
            'success': True,
            'total_companies': stats['total_companies'] or 0,
            'total_reviews': stats['total_reviews'] or 0,
            'avg_rating': round(float(stats['average_rating']), 2) if stats['average_rating'] else 0,
            'companies': companies,
            'sector': gics_value
        })
    
    except Exception as e:
        logger.error(f"Error in get_stats: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/companies', methods=['GET'])
def get_companies():
    """Get all companies with their metrics, optionally filtered by sector"""
    try:
        gics_level, gics_value = get_gics_filter_params()
        company_names = get_companies_for_sector(gics_level=gics_level, gics_value=gics_value)
        
        cached_metrics_map = {}
        conn = get_db_connection()
        if conn and company_names:
            try:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                placeholders = ','.join(['%s'] * len(company_names))
                cursor.execute(f"""
                    SELECT company_name, metrics_json FROM company_metrics_cache
                    WHERE company_name IN ({placeholders})
                """, company_names)
                for row in cursor.fetchall():
                    m = row['metrics_json'] if isinstance(row['metrics_json'], dict) else json.loads(row['metrics_json'])
                    cached_metrics_map[row['company_name']] = m
                cursor.close()
                conn.close()
            except Exception:
                try:
                    conn.close()
                except:
                    pass
        
        companies = []
        uncached_count = 0
        for company_name in company_names:
            metrics = cached_metrics_map.get(company_name)
            if not metrics:
                uncached_count += 1
                if uncached_count <= 50:
                    metrics = get_company_metrics(company_name)
                    if metrics:
                        cache_metrics(company_name, metrics)
                else:
                    continue
            
            if metrics:
                companies.append({
                    'id': company_name.lower().replace(' ', ''),
                    'name': company_name,
                    'total_reviews': metrics['total_reviews'],
                    'overall_rating': metrics['overall_rating'],
                    'culture_values': metrics['culture_values'],
                    'work_life_balance': metrics['work_life_balance'],
                    'career_opportunities': metrics['career_opportunities'],
                    'compensation_benefits': metrics['compensation_benefits'],
                    'senior_management': metrics['senior_management'],
                    'recommend_percentage': metrics['recommend_percentage'],
                    'ceo_approval': metrics['ceo_approval'],
                    'industry': get_company_sector(company_name) or ''
                })
        
        all_ratings = [c['overall_rating'] for c in companies if c['overall_rating']]
        avg_rating = round(mean(all_ratings), 2) if all_ratings else 0
        
        return jsonify({
            'success': True,
            'companies': companies,
            'avg_rating': avg_rating,
            'sector': gics_value
        })
    
    except Exception as e:
        logger.error(f"Error in get_companies: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/culture-profile/<company_name>', methods=['GET'])
def get_culture_profile(company_name):
    """Get culture profile for a specific company"""
    try:
        # Try to get from cache first
        metrics = get_cached_metrics(company_name)
        
        # If not in cache, calculate and cache it
        if not metrics:
            metrics = get_company_metrics(company_name)
            if metrics:
                cache_metrics(company_name, metrics)
        
        if not metrics:
            return jsonify({'success': False, 'error': f'Company {company_name} not found'}), 404
        
        # Ensure confidence_score is calculated
        metrics = calculate_relative_confidence(metrics)
        
        # Replace confidence with confidence_score in response
        hofstede_response = {}
        for dim, data in metrics['hofstede'].items():
            hofstede_response[dim] = {
                'value': data.get('value'),
                'confidence': int(data.get('confidence_score', 0)),
                'confidence_level': data.get('confidence_level')
            }
        
        # Get max values for MIT rescaling — sector-relative so that the best
        # company in the same sector/industry equals 10
        _cg = get_company_gics(company_name)
        _sector_companies = get_companies_for_sector(
            gics_level='sector', gics_value=_cg.get('sector')
        ) if _cg.get('sector') else None
        mit_max_values = get_mit_max_values(_sector_companies)

        mit_response = {}
        for dim, data in metrics['mit_big_9'].items():
            raw_value = data.get('value', 0) or 0
            max_val = mit_max_values.get(dim, 1)
            # Rescale: 10 * (company_value / max_company_value_in_sector)
            rescaled_value = round(10 * (raw_value / max_val), 2) if max_val > 0 else 0
            mit_response[dim] = {
                'value': rescaled_value,  # Use rescaled value as primary
                'raw_value': raw_value,   # Keep raw value for reference
                'confidence': int(data.get('confidence_score', 0)),
                'confidence_level': data.get('confidence_level')
            }
        
        return jsonify({
            'success': True,
            'company_name': metrics['company_name'],
            'hofstede': hofstede_response,
            'mit': mit_response,
            'metadata': {
                'review_count': metrics['total_reviews'],
                'overall_rating': metrics['overall_rating'],
                'overall_confidence': round(min(100, (metrics['total_reviews'] / MIN_REVIEWS_FOR_HIGH_CONFIDENCE) * 100), 1),
                'overall_confidence_level': 'High' if metrics['total_reviews'] >= MIN_REVIEWS_FOR_HIGH_CONFIDENCE else 'Medium' if metrics['total_reviews'] >= MIN_REVIEWS_FOR_MEDIUM_CONFIDENCE else 'Low',
                'analysis_date': datetime.now().isoformat()
            }
        })
    
    except Exception as e:
        logger.error(f"Error in get_culture_profile: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/company/isin/<isin>', methods=['GET'])
def get_company_by_isin(isin):
    """
    Look up a company by ISIN and return its Glassdoor ratings and culture scores.

    Query params:
      include_ratings  (default true)  - include Glassdoor category ratings
      include_culture  (default true)  - include Hofstede / MIT Big 9 scores
    """
    try:
        include_ratings = request.args.get('include_ratings', 'true').lower() != 'false'
        include_culture = request.args.get('include_culture', 'true').lower() != 'false'

        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database unavailable'}), 503
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # ── 1. Resolve ISIN → company details from extraction_queue ──────────
        cur.execute("""
            SELECT isin, issuer_name, ticker, glassdoor_name,
                   gics_sector, gics_industry, gics_sub_industry,
                   status, match_confidence, reviews_extracted
            FROM extraction_queue
            WHERE UPPER(isin) = UPPER(%s)
            LIMIT 1
        """, (isin.strip(),))
        queue_row = cur.fetchone()

        if not queue_row:
            cur.close(); conn.close()
            return jsonify({
                'success': False,
                'error': f'ISIN {isin} not found. Only MSCI-listed companies (2,442) are indexed.',
                'isin': isin
            }), 404

        glassdoor_name = queue_row['glassdoor_name']

        # ── 2. Glassdoor ratings ─────────────────────────────────────────────
        ratings_data = None
        if include_ratings and glassdoor_name:
            cur.execute("""
                SELECT
                    COUNT(*)                                        AS review_count,
                    ROUND(AVG(rating)::numeric, 2)                 AS overall,
                    ROUND(AVG(work_life_balance_rating)::numeric, 2)      AS work_life_balance,
                    ROUND(AVG(career_opportunities_rating)::numeric, 2)   AS career_opportunities,
                    ROUND(AVG(culture_and_values_rating)::numeric, 2)     AS culture_and_values,
                    ROUND(AVG(compensation_and_benefits_rating)::numeric, 2) AS compensation_and_benefits,
                    ROUND(AVG(senior_management_rating)::numeric, 2)      AS senior_management,
                    ROUND(AVG(diversity_and_inclusion_rating)::numeric, 2) AS diversity_and_inclusion,
                    MIN(review_datetime)                            AS earliest_review,
                    MAX(review_datetime)                            AS latest_review
                FROM reviews
                WHERE company_name = %s
            """, (glassdoor_name,))
            row = cur.fetchone()
            if row and row['review_count'] > 0:
                ratings_data = {
                    'review_count': int(row['review_count']),
                    'overall':                  float(row['overall']) if row['overall'] else None,
                    'work_life_balance':         float(row['work_life_balance']) if row['work_life_balance'] else None,
                    'career_opportunities':      float(row['career_opportunities']) if row['career_opportunities'] else None,
                    'culture_and_values':        float(row['culture_and_values']) if row['culture_and_values'] else None,
                    'compensation_and_benefits': float(row['compensation_and_benefits']) if row['compensation_and_benefits'] else None,
                    'senior_management':         float(row['senior_management']) if row['senior_management'] else None,
                    'diversity_and_inclusion':   float(row['diversity_and_inclusion']) if row['diversity_and_inclusion'] else None,
                    'rating_period': {
                        'earliest': row['earliest_review'].isoformat() if row['earliest_review'] else None,
                        'latest':   row['latest_review'].isoformat()   if row['latest_review']   else None,
                    }
                }

        cur.close(); conn.close()

        # ── 3. Culture scores ────────────────────────────────────────────────
        hofstede_data = None
        mit_data = None
        if include_culture and glassdoor_name:
            metrics = get_cached_metrics(glassdoor_name)
            if not metrics:
                metrics = get_company_metrics(glassdoor_name)
                if metrics:
                    cache_metrics(glassdoor_name, metrics)

            if metrics:
                metrics = calculate_relative_confidence(metrics)
                _isin_sector = queue_row.get('gics_sector') or ''
                _isin_sector_cos = get_companies_for_sector(
                    gics_level='sector', gics_value=_isin_sector
                ) if _isin_sector else None
                mit_max = get_mit_max_values(_isin_sector_cos)

                hofstede_data = {}
                for dim, d in metrics['hofstede'].items():
                    hofstede_data[dim] = {
                        'value':            d.get('value'),
                        'confidence':       int(d.get('confidence_score', 0)),
                        'confidence_level': d.get('confidence_level'),
                    }

                mit_data = {}
                for dim, d in metrics['mit_big_9'].items():
                    raw = d.get('value', 0) or 0
                    max_val = mit_max.get(dim, 1)
                    rescaled = round(10 * (raw / max_val), 2) if max_val > 0 else 0
                    mit_data[dim] = {
                        'value':            rescaled,
                        'raw_value':        raw,
                        'confidence':       int(d.get('confidence_score', 0)),
                        'confidence_level': d.get('confidence_level'),
                    }

        # ── 4. Determine data availability ──────────────────────────────────
        has_ratings = ratings_data is not None
        has_culture = hofstede_data is not None
        if has_ratings and has_culture:
            availability = 'full'
        elif has_ratings:
            availability = 'ratings_only'
        elif has_culture:
            availability = 'culture_only'
        elif glassdoor_name:
            availability = 'matched_no_reviews'
        else:
            availability = 'not_matched'

        response = {
            'success': True,
            'isin':           queue_row['isin'],
            'issuer_name':    queue_row['issuer_name'],
            'glassdoor_name': glassdoor_name,
            'ticker':         queue_row['ticker'],
            'gics': {
                'sector':       queue_row['gics_sector'],
                'industry':     queue_row['gics_industry'],
                'sub_industry': queue_row['gics_sub_industry'],
            },
            'extraction_status': {
                'status':           queue_row['status'],
                'match_confidence': queue_row['match_confidence'],
                'reviews_extracted': queue_row['reviews_extracted'],
            },
            'data_availability': availability,
        }
        if include_ratings:
            response['glassdoor_ratings'] = ratings_data
        if include_culture:
            response['culture_scores'] = {
                'hofstede': hofstede_data,
                'mit_big_9': mit_data,
            }

        return jsonify(response)

    except Exception as e:
        logger.error(f"Error in get_company_by_isin: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/company/search', methods=['GET'])
def search_company():
    """
    Search for companies by name, ticker, or ISIN prefix.
    Query params:
      q        - search term (required, min 2 chars)
      limit    - max results to return (default 20, max 100)
    """
    try:
        q = request.args.get('q', '').strip()
        limit = min(int(request.args.get('limit', 20)), 100)

        if len(q) < 2:
            return jsonify({'success': False, 'error': 'Query must be at least 2 characters'}), 400

        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            SELECT isin, issuer_name, ticker, glassdoor_name,
                   gics_sector, gics_industry, status, reviews_extracted
            FROM extraction_queue
            WHERE issuer_name ILIKE %s
               OR ticker ILIKE %s
               OR isin ILIKE %s
               OR glassdoor_name ILIKE %s
            ORDER BY
                CASE WHEN UPPER(isin) = UPPER(%s) THEN 0
                     WHEN UPPER(ticker) = UPPER(%s) THEN 1
                     ELSE 2 END,
                reviews_extracted DESC NULLS LAST
            LIMIT %s
        """, (f'%{q}%', f'%{q}%', f'%{q}%', f'%{q}%', q, q, limit))

        rows = cur.fetchall()
        cur.close(); conn.close()

        return jsonify({
            'success': True,
            'query': q,
            'count': len(rows),
            'results': [dict(r) for r in rows]
        })

    except Exception as e:
        logger.error(f"Error in search_company: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/industry-average', methods=['GET'])
def get_industry_average():
    """Get industry average culture profile, optionally filtered by sector"""
    try:
        gics_level, gics_value = get_gics_filter_params()
        company_names = get_companies_for_sector(gics_level=gics_level, gics_value=gics_value)
        
        cached_metrics_map = {}
        conn = get_db_connection()
        if conn and company_names:
            try:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                placeholders = ','.join(['%s'] * len(company_names))
                cursor.execute(f"""
                    SELECT company_name, metrics_json FROM company_metrics_cache
                    WHERE company_name IN ({placeholders})
                """, company_names)
                for row in cursor.fetchall():
                    m = row['metrics_json'] if isinstance(row['metrics_json'], dict) else json.loads(row['metrics_json'])
                    cached_metrics_map[row['company_name']] = m
                cursor.close()
                conn.close()
            except Exception:
                try:
                    conn.close()
                except:
                    pass
        
        hofstede_avg = {dim: [] for dim in HOFSTEDE_DIMENSIONS}
        mit_avg = {dim: [] for dim in MIT_DIMENSIONS}
        total_reviews = 0
        all_company_metrics = []
        
        for company_name in company_names:
            metrics = cached_metrics_map.get(company_name)
            if not metrics:
                metrics = get_company_metrics(company_name)
                if metrics:
                    cache_metrics(company_name, metrics)
            
            if metrics:
                all_company_metrics.append(metrics)
                total_reviews += metrics.get('total_reviews', 0)
                for dim in HOFSTEDE_DIMENSIONS:
                    val = metrics.get('hofstede', {}).get(dim, {}).get('value', 0)
                    hofstede_avg[dim].append(val)
                for dim in MIT_DIMENSIONS:
                    val = metrics.get('mit_big_9', {}).get(dim, {}).get('value', 0)
                    mit_avg[dim].append(val)
        
        hofstede_result = {}
        mit_result = {}
        
        # Get max evidence values for normalization
        hofstede_max_evidence = {}
        mit_max_evidence = {}
        for metrics in all_company_metrics:
            for dim in HOFSTEDE_DIMENSIONS:
                evidence = metrics.get('hofstede', {}).get(dim, {}).get('total_evidence', 0)
                hofstede_max_evidence[dim] = max(hofstede_max_evidence.get(dim, 0), evidence)
            for dim in MIT_DIMENSIONS:
                evidence = metrics.get('mit_big_9', {}).get(dim, {}).get('total_evidence', 0)
                mit_max_evidence[dim] = max(mit_max_evidence.get(dim, 0), evidence)
        
        for dim in HOFSTEDE_DIMENSIONS:
            if hofstede_avg[dim]:
                avg_val = mean(hofstede_avg[dim])
                # Calculate average confidence for industry
                avg_confidence = mean([m.get('hofstede', {}).get(dim, {}).get('confidence_score', 0) or 0 for m in all_company_metrics if m.get('hofstede', {}).get(dim)])
                hofstede_result[dim] = {'value': round(avg_val, 3), 'confidence': round(avg_confidence, 1), 'confidence_level': 'High' if avg_confidence >= 50 else 'Medium' if avg_confidence >= 25 else 'Low'}
        
        # Get max values for MIT rescaling — use companies in the current GICS filter
        mit_max_values = get_mit_max_values(company_names)

        for dim in MIT_DIMENSIONS:
            if mit_avg[dim]:
                raw_value = mean(mit_avg[dim])
                max_val = mit_max_values.get(dim, 1)
                # Rescale: 10 * (company_value / max_company_value)
                rescaled_value = round(10 * (raw_value / max_val), 2) if max_val > 0 else 0
                # Calculate average confidence for industry
                avg_confidence = mean([m.get('mit_big_9', {}).get(dim, {}).get('confidence_score', 0) or 0 for m in all_company_metrics if m.get('mit_big_9', {}).get(dim)])
                mit_result[dim] = {
                    'value': rescaled_value,
                    'raw_value': round(raw_value, 4),
                    'confidence': round(avg_confidence, 1),
                    'confidence_level': 'High' if avg_confidence >= 50 else 'Medium' if avg_confidence >= 25 else 'Low'
                }
        
        # Calculate overall average confidence
        all_hof_conf = [v.get('confidence', 0) for v in hofstede_result.values()]
        all_mit_conf = [v.get('confidence', 0) for v in mit_result.values()]
        overall_conf = mean(all_hof_conf + all_mit_conf) if (all_hof_conf + all_mit_conf) else 0
        
        return jsonify({
            'success': True,
            'company_name': f'{gics_value} Average' if gics_value else 'Industry Average',
            'hofstede': hofstede_result,
            'mit': mit_result,
            'sector': gics_value,
            'metadata': {
                'review_count': total_reviews,
                'overall_rating': 0,
                'overall_confidence': round(overall_conf, 1),
                'overall_confidence_level': 'High' if overall_conf >= 50 else 'Medium' if overall_conf >= 25 else 'Low',
                'analysis_date': datetime.now().isoformat()
            }
        })
    
    except Exception as e:
        logger.error(f"Error in get_industry_average: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/quarterly-trends', methods=['GET'])
def get_quarterly_trends():
    """Get quarterly trends for a company"""
    try:
        company_name = request.args.get('company')
        rating_type = request.args.get('rating_type', 'overall_rating')
        
        if not company_name:
            return jsonify({'success': False, 'error': 'company parameter required'}), 400
        
        # Try to get from cache first
        metrics = get_cached_metrics(company_name)
        
        # If not in cache, calculate and cache it
        if not metrics:
            metrics = get_company_metrics(company_name)
            if metrics:
                cache_metrics(company_name, metrics)
        
        if not metrics:
            return jsonify({'success': False, 'error': f'Company {company_name} not found'}), 404
        
        # Map dimension parameter to database column
        # Frontend sends: 'overall', 'culture', 'worklife', 'compensation', 'career', 'management'
        rating_column_map = {
            'overall': 'rating',
            'culture': 'culture_and_values_rating',
            'worklife': 'work_life_balance_rating',
            'compensation': 'compensation_and_benefits_rating',
            'career': 'career_opportunities_rating',
            'management': 'senior_management_rating'
        }
        
        # Get dimension from query parameter (frontend sends 'dimension')
        dimension = request.args.get('dimension', 'overall')
        rating_column = rating_column_map.get(dimension, 'rating')
        
        # Query quarterly trends from database
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get quarterly aggregated data
        query = f"""
            SELECT 
                DATE_TRUNC('quarter', review_datetime) as quarter,
                AVG({rating_column}) as avg_rating,
                COUNT(*) as review_count,
                MIN({rating_column}) as min_rating,
                MAX({rating_column}) as max_rating
            FROM reviews
            WHERE company_name = %s AND {rating_column} IS NOT NULL
            GROUP BY DATE_TRUNC('quarter', review_datetime)
            ORDER BY quarter ASC
        """
        
        cursor.execute(query, (company_name,))
        quarterly_data = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # Format the data for the frontend
        # Minimum reviews per quarter to include in trends
        MIN_REVIEWS_PER_QUARTER = 5
        
        main_trends = []
        for row in quarterly_data:  # Already sorted ASC (oldest first = left side of chart)
            if row['quarter']:
                # Skip quarters with fewer than minimum reviews
                if row['review_count'] < MIN_REVIEWS_PER_QUARTER:
                    continue
                    
                # Extract year and quarter
                quarter_date = row['quarter']
                year = quarter_date.year
                month = quarter_date.month
                quarter_num = (month - 1) // 3 + 1
                
                main_trends.append({
                    'quarter': f"Q{quarter_num} {year}",
                    'avg_rating': round(float(row['avg_rating']), 2) if row['avg_rating'] else 0,
                    'review_count': row['review_count']
                })
        
        return jsonify({
            'success': True,
            'company': metrics['company_name'],  # Frontend expects 'company' not 'company_name'
            'dimension': dimension,
            'main_trends': main_trends  # Frontend expects 'main_trends' not 'data'
        })
    
    except Exception as e:
        logger.error(f"Error in get_quarterly_trends: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/companies-list', methods=['GET'])
def get_companies_list():
    """Get list of all companies for dropdown menus, optionally filtered by sector"""
    try:
        gics_level, gics_value = get_gics_filter_params()
        companies = get_companies_for_sector(gics_level=gics_level, gics_value=gics_value)
        
        return jsonify({
            'success': True,
            'companies': companies,
            'sector': gics_value
        })
    
    except Exception as e:
        logger.error(f"Error in get_companies_list: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/culture-comparison', methods=['POST'])
def culture_comparison():
    """Compare culture profiles between two companies"""
    try:
        data = request.json
        company1 = data.get('company1')
        company2 = data.get('company2')
        
        if not company1 or not company2:
            return jsonify({'success': False, 'error': 'Both companies required'}), 400
        
        # Get profiles for both companies
        profile1 = get_company_metrics(company1)
        profile2 = get_company_metrics(company2)
        
        if not profile1 or not profile2:
            return jsonify({'success': False, 'error': 'One or both companies not found'}), 404
        
        # Calculate differences
        hofstede_diff = {}
        mit_diff = {}
        
        for dim in HOFSTEDE_DIMENSIONS:
            val1 = profile1.get('hofstede', {}).get(dim, {}).get('value', 0)
            val2 = profile2.get('hofstede', {}).get(dim, {}).get('value', 0)
            hofstede_diff[dim] = {
                'company1': val1,
                'company2': val2,
                'difference': val2 - val1
            }
        
        # Get max values for MIT rescaling to 0-10 scale
        mit_max_values = get_mit_max_values()
        
        for dim in MIT_DIMENSIONS:
            raw_val1 = profile1.get('mit_big_9', {}).get(dim, {}).get('value', 0) or 0
            raw_val2 = profile2.get('mit_big_9', {}).get(dim, {}).get('value', 0) or 0
            max_val = mit_max_values.get(dim, 1)
            # Rescale both values so max company = 10
            val1 = round(10 * (raw_val1 / max_val), 2) if max_val > 0 else 0
            val2 = round(10 * (raw_val2 / max_val), 2) if max_val > 0 else 0
            mit_diff[dim] = {
                'company1': val1,
                'company2': val2,
                'difference': val2 - val1
            }
        
        return jsonify({
            'success': True,
            'company1': company1,
            'company2': company2,
            'hofstede_comparison': hofstede_diff,
            'mit_comparison': mit_diff
        })
    
    except Exception as e:
        logger.error(f"Error in culture_comparison: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/culture-trends/<company_name>', methods=['GET'])
def culture_trends(company_name):
    """Get culture dimension trends over time for a company"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get reviews by quarter
        cursor.execute("""
            SELECT 
                DATE_TRUNC('quarter', review_datetime)::date as quarter,
                COUNT(*) as review_count
            FROM reviews
            WHERE company_name = %s
            GROUP BY DATE_TRUNC('quarter', review_datetime)
            ORDER BY quarter ASC
        """, (company_name,))
        
        quarters = cursor.fetchall()
        trends = {}
        
        for quarter_data in quarters:
            quarter = quarter_data['quarter'].isoformat()
            review_count = quarter_data['review_count']
            
            if review_count < 5:
                continue
            
            # Get reviews for this quarter
            cursor.execute("""
                SELECT review_text FROM reviews
                WHERE company_name = %s
                AND DATE_TRUNC('quarter', review_datetime)::date = %s
                LIMIT 50
            """, (company_name, quarter_data['quarter']))
            
            quarter_reviews = [row['review_text'] for row in cursor.fetchall()]
            
            # Score dimensions for this quarter
            hofstede_scores = {dim: [] for dim in HOFSTEDE_DIMENSIONS}
            mit_scores = {dim: [] for dim in MIT_DIMENSIONS}
            
            for review_text in quarter_reviews:
                scores = score_review_with_dictionary(review_text)
                for dim in HOFSTEDE_DIMENSIONS:
                    if dim in scores['hofstede']:
                        hofstede_scores[dim].append(scores['hofstede'][dim])
                for dim in MIT_DIMENSIONS:
                    if dim in scores['mit_big_9']:
                        mit_scores[dim].append(scores['mit_big_9'][dim])
            
            # Calculate averages
            trends[quarter] = {
                'review_count': review_count,
                'hofstede': {dim: mean(scores) if scores else 0 for dim, scores in hofstede_scores.items()},
                'mit_big_9': {dim: mean(scores) if scores else 0 for dim, scores in mit_scores.items()}
            }
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'company': company_name,
            'trends': trends
        })
    
    except Exception as e:
        logger.error(f"Error in culture_trends: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/claude-insights/<company_name>', methods=['GET'])
def claude_insights(company_name):
    """Get AI-generated insights about company culture (stub)"""
    try:
        profile = get_company_metrics(company_name)
        if not profile:
            return jsonify({'success': False, 'error': 'Company not found'}), 404
        
        # Generate insights based on dimension scores
        hofstede = profile.get('hofstede', {})
        mit = profile.get('mit_big_9', {})
        
        # Identify strongest and weakest dimensions
        hofstede_values = [(dim, data.get('value', 0)) for dim, data in hofstede.items()]
        mit_values = [(dim, data.get('value', 0)) for dim, data in mit.items()]
        
        hofstede_values.sort(key=lambda x: abs(x[1]), reverse=True)
        mit_values.sort(key=lambda x: x[1], reverse=True)
        
        # Create basic insights
        insights = {
            'strengths': [],
            'areas_for_improvement': [],
            'summary': f"Culture analysis for {company_name} based on {profile.get('review_count', 0)} reviews."
        }
        
        # Identify strengths (high positive values)
        for dim, value in hofstede_values[:3]:
            if value > 0.5:
                insights['strengths'].append(f"{dim.replace('_', ' ').title()}: {value:.2f}")
        
        for dim, value in mit_values[:3]:
            if value > 6:
                insights['strengths'].append(f"{dim.replace('_', ' ').title()}: {value:.2f}")
        
        # Identify areas for improvement (low values)
        for dim, value in hofstede_values[-3:]:
            if value < -0.5:
                insights['areas_for_improvement'].append(f"{dim.replace('_', ' ').title()}: {value:.2f}")
        
        for dim, value in mit_values[-3:]:
            if value < 4:
                insights['areas_for_improvement'].append(f"{dim.replace('_', ' ').title()}: {value:.2f}")
        
        return jsonify({
            'success': True,
            'company': company_name,
            'insights': insights
        })
    
    except Exception as e:
        logger.error(f"Error in claude_insights: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/culture-benchmarking/<company_name>', methods=['GET'])
def culture_benchmarking(company_name):
    """Get benchmarking data comparing company to sector/industry averages"""
    try:
        gics_level, gics_value = get_gics_filter_params()
        if not gics_value:
            gics_value = get_company_sector(company_name)
        
        company_profile = get_company_metrics(company_name)
        if not company_profile:
            return jsonify({'success': False, 'error': 'Company not found'}), 404
        
        all_companies = get_companies_for_sector(gics_level=gics_level, gics_value=gics_value)
        other_companies = [c for c in all_companies if c != company_name]
        
        cached_map = get_cached_metrics_batch(other_companies)
        
        hofstede_avg = {dim: [] for dim in HOFSTEDE_DIMENSIONS}
        mit_avg = {dim: [] for dim in MIT_DIMENSIONS}
        
        for other_company in other_companies:
            other_profile = cached_map.get(other_company)
            if not other_profile:
                other_profile = get_company_metrics(other_company)
                if other_profile:
                    cache_metrics(other_company, other_profile)
            if other_profile:
                for dim in HOFSTEDE_DIMENSIONS:
                    val = other_profile.get('hofstede', {}).get(dim, {}).get('value', 0)
                    hofstede_avg[dim].append(val)
                for dim in MIT_DIMENSIONS:
                    val = other_profile.get('mit_big_9', {}).get(dim, {}).get('value', 0)
                    mit_avg[dim].append(val)
        
        # Calculate means
        hofstede_industry = {dim: mean(vals) if vals else 0 for dim, vals in hofstede_avg.items()}
        mit_industry = {dim: mean(vals) if vals else 0 for dim, vals in mit_avg.items()}
        
        # Calculate percentiles
        hofstede_percentiles = {}
        mit_percentiles = {}
        
        for dim in HOFSTEDE_DIMENSIONS:
            company_val = company_profile.get('hofstede', {}).get(dim, {}).get('value', 0)
            industry_vals = sorted(hofstede_avg[dim])
            if industry_vals:
                percentile = (sum(1 for v in industry_vals if v <= company_val) / len(industry_vals)) * 100
                hofstede_percentiles[dim] = percentile
        
        for dim in MIT_DIMENSIONS:
            company_val = company_profile.get('mit_big_9', {}).get(dim, {}).get('value', 0)
            industry_vals = sorted(mit_avg[dim])
            if industry_vals:
                percentile = (sum(1 for v in industry_vals if v <= company_val) / len(industry_vals)) * 100
                mit_percentiles[dim] = percentile
        
        return jsonify({
            'success': True,
            'company': company_name,
            'sector': gics_value,
            'hofstede_benchmarking': {
                'company': {dim: company_profile.get('hofstede', {}).get(dim, {}).get('value', 0) for dim in HOFSTEDE_DIMENSIONS},
                'industry_average': hofstede_industry,
                'percentile': hofstede_percentiles
            },
            'mit_benchmarking': {
                'company': {dim: company_profile.get('mit_big_9', {}).get(dim, {}).get('value', 0) for dim in MIT_DIMENSIONS},
                'industry_average': mit_industry,
                'percentile': mit_percentiles
            }
        })
    
    except Exception as e:
        logger.error(f"Error in culture_benchmarking: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# PERFORMANCE CORRELATION API
# ============================================================================

def _load_fmp_perf_map():
    """Load all rows from fmp_performance_metrics as a company→dict map."""
    fmp_map = {}
    try:
        conn = get_db_connection()
        if conn:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT company_name, roe_latest, roe_5y_avg,
                       op_margin_latest, op_margin_5y_avg,
                       net_margin_latest, tsr_5y, revenue_growth_5y,
                       market_cap, data_source, gics_sector, gics_industry
                FROM fmp_performance_metrics
            """)
            for row in cur.fetchall():
                fmp_map[row['company_name']] = dict(row)
            cur.close()
            conn.close()
    except Exception as e:
        logger.warning(f"Could not load fmp_performance_metrics: {e}")
    return fmp_map


_GICS_INDUSTRY_TO_BUSINESS_MODEL = {
    'Banks': 'Traditional',
    'Insurance': 'Insurance/Wealth',
    'Diversified Financials': 'Traditional',
    'Financial Services': 'Traditional',
}

def _fmp_row_to_perf_metrics(company, fmp_row):
    """Convert a fmp_performance_metrics row to the performance_analyzer metrics format."""
    data_source = fmp_row.get('data_source', 'fmp')
    business_model = performance_analyzer.get_business_model(company)
    if business_model == 'Unknown':
        gics_industry = fmp_row.get('gics_industry') or ''
        business_model = _GICS_INDUSTRY_TO_BUSINESS_MODEL.get(gics_industry,
                         'Listed' if data_source == 'fmp' else 'Traditional')
    raw = {
        'company': company,
        'matched_name': company,
        'business_model': business_model,
        'roe_5y_avg': fmp_row.get('roe_5y_avg') or fmp_row.get('roe_latest'),
        'op_margin_5y_avg': fmp_row.get('op_margin_5y_avg') or fmp_row.get('op_margin_latest'),
        'tsr_cagr_5y': fmp_row.get('tsr_5y'),
        'revenue_growth_5y': fmp_row.get('revenue_growth_5y'),
        'roe_latest': fmp_row.get('roe_latest'),
        'op_margin_latest': fmp_row.get('op_margin_latest'),
        'market_cap': fmp_row.get('market_cap'),
    }
    return {k: v for k, v in raw.items() if v is not None}


_FINANCIAL_METRIC_KEYS = {'roe_5y_avg', 'aum_cagr_5y', 'tsr_cagr_5y', 'op_margin_5y_avg',
                          'revenue_growth_5y', 'roe_latest', 'op_margin_latest'}

def _has_financial_metrics(metrics):
    """Return True if the metrics dict contains at least one actual financial value."""
    if not metrics:
        return False
    return any(metrics.get(k) is not None for k in _FINANCIAL_METRIC_KEYS)


def _get_perf_metrics_with_fmp_fallback(company, fmp_perf_map):
    """Get performance metrics for a company, falling back to FMP data if Excel has none."""
    perf_metrics = performance_analyzer.get_performance_metrics(company)
    if not _has_financial_metrics(perf_metrics):
        fmp_row = fmp_perf_map.get(company)
        if fmp_row:
            fmp_metrics = _fmp_row_to_perf_metrics(company, fmp_row)
            if perf_metrics:
                perf_metrics = {**perf_metrics, **fmp_metrics}
            else:
                perf_metrics = fmp_metrics
    return perf_metrics


@app.route('/api/performance-correlation', methods=['GET'])
def get_performance_correlation():
    """Get correlation analysis between culture metrics and business performance"""
    try:
        gics_level, gics_value = get_gics_filter_params()
        
        if not performance_analyzer.loaded:
            performance_analyzer.load_data()
        
        culture_companies = get_companies_for_sector(gics_level=gics_level, gics_value=gics_value)
        fmp_perf_map = _load_fmp_perf_map()
        
        cached_map = get_cached_metrics_batch(culture_companies)
        
        culture_data = []
        performance_data = []
        
        peer_stats = performance_analyzer.get_peer_statistics()
        
        for company in culture_companies:
            metrics = cached_map.get(company)
            if not metrics:
                metrics = get_company_metrics(company)
                if metrics:
                    cache_metrics(company, metrics)
            
            if metrics:
                culture_data.append({
                    'company': company,
                    'hofstede': metrics.get('hofstede', {}),
                    'mit': metrics.get('mit_big_9', {})
                })
            
            perf_metrics = _get_perf_metrics_with_fmp_fallback(company, fmp_perf_map)
            
            if _has_financial_metrics(perf_metrics):
                perf_metrics['composite_score'] = performance_analyzer.calculate_composite_score(
                    perf_metrics, peer_stats
                )
                performance_data.append(perf_metrics)
        
        correlations = performance_analyzer.calculate_correlation(culture_data, performance_data)
        
        return jsonify({
            'success': True,
            'correlations': correlations,
            'companies_with_both': len([p for p in performance_data if p.get('composite_score')]),
            'culture_companies': len(culture_companies),
            'performance_companies': len(performance_data),
            'sector': gics_value
        })
    
    except Exception as e:
        logger.error(f"Error in performance correlation: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/company-performance', methods=['GET'])
def get_company_performance():
    """Get performance data for a specific company"""
    try:
        company_name = request.args.get('company')
        if not company_name:
            return jsonify({'success': False, 'error': 'company parameter required'}), 400
        
        if not performance_analyzer.loaded:
            performance_analyzer.load_data()
        
        fmp_perf_map_cp = _load_fmp_perf_map()
        perf_metrics = _get_perf_metrics_with_fmp_fallback(company_name, fmp_perf_map_cp)
        
        if not _has_financial_metrics(perf_metrics):
            return jsonify({'success': False, 'error': f'No performance data for {company_name}'}), 404
        
        # Calculate composite score
        peer_stats = performance_analyzer.get_peer_statistics(perf_metrics.get('business_model'))
        perf_metrics['composite_score'] = performance_analyzer.calculate_composite_score(perf_metrics, peer_stats)
        perf_metrics['peer_stats'] = peer_stats
        
        return jsonify({
            'success': True,
            'performance': perf_metrics
        })
    
    except Exception as e:
        logger.error(f"Error getting company performance: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/performance-rankings', methods=['GET'])
def get_performance_rankings():
    """Get ranked list of companies by composite performance score"""
    try:
        gics_level, gics_value = get_gics_filter_params()
        
        if not performance_analyzer.loaded:
            performance_analyzer.load_data()
        
        culture_companies = get_companies_for_sector(gics_level=gics_level, gics_value=gics_value)
        fmp_perf_map = _load_fmp_perf_map()
        
        cached_map = get_cached_metrics_batch(culture_companies)
        
        rankings = []
        peer_stats = performance_analyzer.get_peer_statistics()
        
        for company in culture_companies:
            perf_metrics = _get_perf_metrics_with_fmp_fallback(company, fmp_perf_map)
            
            if _has_financial_metrics(perf_metrics):
                composite = performance_analyzer.calculate_composite_score(perf_metrics, peer_stats)
                if composite is not None:
                    culture_metrics = cached_map.get(company)
                    if not culture_metrics:
                        culture_metrics = get_company_metrics(company)
                    
                    aum_raw = perf_metrics.get('aum_cagr_5y')
                    rankings.append({
                        'company': company,
                        'composite_score': round(composite, 1),
                        'business_model': perf_metrics.get('business_model', 'Unknown'),
                        'sector': get_company_sector(company) or '',
                        'roe_5y_avg': perf_metrics.get('roe_5y_avg'),
                        'aum_cagr_5y': round(aum_raw * 100, 1) if aum_raw else None,
                        'tsr_cagr_5y': perf_metrics.get('tsr_cagr_5y'),
                        'culture_confidence': culture_metrics.get('overall_confidence', 0) if culture_metrics else 0
                    })
        
        rankings.sort(key=lambda x: x['composite_score'], reverse=True)
        
        for i, r in enumerate(rankings):
            r['rank'] = i + 1
        
        return jsonify({
            'success': True,
            'rankings': rankings,
            'total': len(rankings),
            'sector': gics_value
        })
    
    except Exception as e:
        logger.error(f"Error getting performance rankings: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# ERROR HANDLERS
# ============================================================================

@app.route('/api/company-analysis/<company_name>', methods=['GET'])
def get_company_analysis(company_name):
    """Get company analysis with culture scores, industry averages, and correlations"""
    try:
        gics_level, gics_value = get_gics_filter_params()
        if not gics_value:
            gics_value = get_company_sector(company_name)
        
        metrics = get_cached_metrics(company_name)
        if not metrics:
            metrics = get_company_metrics(company_name)
            if metrics:
                cache_metrics(company_name, metrics)
        
        if not metrics:
            return jsonify({'success': False, 'error': 'Company not found'}), 404
        
        company_names = get_companies_for_sector(gics_level=gics_level, gics_value=gics_value)
        
        cached_map = get_cached_metrics_batch(company_names)
        
        hofstede_avg = {dim: [] for dim in HOFSTEDE_DIMENSIONS}
        mit_avg = {dim: [] for dim in MIT_DIMENSIONS}
        
        for name in company_names:
            m = cached_map.get(name)
            if not m:
                m = get_company_metrics(name)
                if m:
                    cache_metrics(name, m)
            if m:
                for dim in HOFSTEDE_DIMENSIONS:
                    val = m.get('hofstede', {}).get(dim, {}).get('value', 0)
                    hofstede_avg[dim].append(val)
                for dim in MIT_DIMENSIONS:
                    val = m.get('mit_big_9', {}).get(dim, {}).get('value', 0)
                    mit_avg[dim].append(val)
        
        industry_hofstede = {}
        industry_mit = {}
        
        for dim in HOFSTEDE_DIMENSIONS:
            if hofstede_avg[dim]:
                industry_hofstede[dim] = round(mean(hofstede_avg[dim]), 3)
        
        mit_max_values = get_mit_max_values(company_names)
        for dim in MIT_DIMENSIONS:
            if mit_avg[dim]:
                raw_avg = mean(mit_avg[dim])
                max_val = mit_max_values.get(dim, 1)
                industry_mit[dim] = round(10 * (raw_avg / max_val), 2) if max_val > 0 else 0
        
        if not performance_analyzer.loaded:
            performance_analyzer.load_data()
        
        fmp_perf_map_ca = _load_fmp_perf_map()
        culture_data = []
        performance_data = []
        peer_stats = performance_analyzer.get_peer_statistics()
        
        for name in company_names:
            m = cached_map.get(name)
            if not m:
                m = get_company_metrics(name)
            if m:
                culture_data.append({
                    'company': name,
                    'hofstede': m.get('hofstede', {}),
                    'mit': m.get('mit_big_9', {})
                })
            perf_metrics = _get_perf_metrics_with_fmp_fallback(name, fmp_perf_map_ca)
            if _has_financial_metrics(perf_metrics):
                perf_metrics['composite_score'] = performance_analyzer.calculate_composite_score(
                    perf_metrics, peer_stats
                )
                performance_data.append(perf_metrics)
        
        correlations = performance_analyzer.calculate_correlation(culture_data, performance_data)
        
        # Extract correlations for each dimension with composite_score
        # Structure: correlations['hofstede'][dim]['composite_score']['correlation']
        hofstede_correlations = {}
        mit_correlations = {}
        
        hofstede_corr_data = correlations.get('hofstede', {})
        mit_corr_data = correlations.get('mit', {})
        
        for dim in HOFSTEDE_DIMENSIONS:
            dim_data = hofstede_corr_data.get(dim, {}).get('composite_score', {})
            hofstede_correlations[dim] = dim_data.get('correlation', 0) if isinstance(dim_data, dict) else 0
        for dim in MIT_DIMENSIONS:
            dim_data = mit_corr_data.get(dim, {}).get('composite_score', {})
            mit_correlations[dim] = dim_data.get('correlation', 0) if isinstance(dim_data, dict) else 0
        
        # Format company scores
        company_hofstede = {}
        company_mit = {}
        
        for dim in HOFSTEDE_DIMENSIONS:
            company_hofstede[dim] = metrics.get('hofstede', {}).get(dim, {}).get('value', 0)
        
        for dim in MIT_DIMENSIONS:
            raw_val = metrics.get('mit_big_9', {}).get(dim, {}).get('value', 0)
            max_val = mit_max_values.get(dim, 1)
            company_mit[dim] = round(10 * (raw_val / max_val), 2) if max_val > 0 else 0
        
        # Calculate culture scores: Σ(correlation × deviation from industry average)
        # Positive score = culture dimensions positively aligned with performance
        # Negative score = culture dimensions negatively aligned with performance
        # Also calculate weighted confidence: Σ(confidence × |correlation|) / Σ(|correlation|)
        hofstede_score = 0.0
        hofstede_weighted_conf_sum = 0.0
        hofstede_weight_sum = 0.0
        
        for dim in HOFSTEDE_DIMENSIONS:
            company_val = company_hofstede.get(dim, 0)
            industry_val = industry_hofstede.get(dim, 0)
            correlation = hofstede_correlations.get(dim, 0)
            deviation = company_val - industry_val
            hofstede_score += correlation * deviation
            
            # Get dimension confidence (0-100 scale)
            conf_score = metrics.get('hofstede', {}).get(dim, {}).get('confidence_score', 0) or 0
            conf_normalized = conf_score / 100.0
            
            # Weight by |correlation| = sqrt(correlation^2)
            weight = abs(correlation)
            hofstede_weighted_conf_sum += conf_normalized * weight
            hofstede_weight_sum += weight
        
        hofstede_confidence = (hofstede_weighted_conf_sum / hofstede_weight_sum * 100) if hofstede_weight_sum > 0 else 0
        
        mit_score = 0.0
        mit_weighted_conf_sum = 0.0
        mit_weight_sum = 0.0
        
        for dim in MIT_DIMENSIONS:
            company_val = company_mit.get(dim, 0)
            industry_val = industry_mit.get(dim, 0)
            correlation = mit_correlations.get(dim, 0)
            deviation = company_val - industry_val
            mit_score += correlation * deviation
            
            # Get dimension confidence (0-100 scale)
            conf_score = metrics.get('mit_big_9', {}).get(dim, {}).get('confidence_score', 0) or 0
            conf_normalized = conf_score / 100.0
            
            # Weight by |correlation| = sqrt(correlation^2)
            weight = abs(correlation)
            mit_weighted_conf_sum += conf_normalized * weight
            mit_weight_sum += weight
        
        mit_confidence = (mit_weighted_conf_sum / mit_weight_sum * 100) if mit_weight_sum > 0 else 0
        
        # Combined score is sum of both (Hofstede is -1 to +1 scale, MIT is 0-10 scale)
        # Scale Hofstede to match MIT magnitude roughly (multiply by 5)
        combined_score = (hofstede_score * 5) + mit_score
        
        # Combined confidence: weighted average of Hofstede and MIT confidences
        total_weight = hofstede_weight_sum + mit_weight_sum
        if total_weight > 0:
            combined_confidence = (
                (hofstede_confidence * hofstede_weight_sum) + 
                (mit_confidence * mit_weight_sum)
            ) / total_weight
        else:
            combined_confidence = 0
        
        return jsonify({
            'success': True,
            'company_name': company_name,
            'sector': gics_value,
            'company': {
                'hofstede': company_hofstede,
                'mit': company_mit
            },
            'industry_average': {
                'hofstede': industry_hofstede,
                'mit': industry_mit
            },
            'correlations': {
                'hofstede': hofstede_correlations,
                'mit': mit_correlations
            },
            'culture_scores': {
                'hofstede': round(hofstede_score, 3),
                'mit': round(mit_score, 3),
                'combined': round(combined_score, 3),
                'hofstede_confidence': round(hofstede_confidence, 1),
                'mit_confidence': round(mit_confidence, 1),
                'combined_confidence': round(combined_confidence, 1)
            },
            'metadata': {
                'review_count': metrics.get('total_reviews', 0),
                'overall_rating': metrics.get('overall_rating', 0)
            }
        })
    
    except Exception as e:
        logger.error(f"Error in company analysis: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/company-culture-trend/<company_name>', methods=['GET'])
def get_company_culture_trend(company_name):
    """Get quarterly culture rating trend for a company vs industry average"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get company quarterly ratings
        cursor.execute("""
            SELECT 
                EXTRACT(YEAR FROM review_datetime) as year,
                EXTRACT(QUARTER FROM review_datetime) as quarter,
                AVG(culture_and_values_rating) as avg_culture_rating,
                COUNT(*) as review_count
            FROM reviews
            WHERE company_name = %s 
              AND culture_and_values_rating IS NOT NULL
              AND review_datetime IS NOT NULL
            GROUP BY year, quarter
            ORDER BY year, quarter
        """, (company_name,))
        company_data = cursor.fetchall()
        
        # Get industry quarterly averages
        cursor.execute("""
            SELECT 
                EXTRACT(YEAR FROM review_datetime) as year,
                EXTRACT(QUARTER FROM review_datetime) as quarter,
                AVG(culture_and_values_rating) as avg_culture_rating,
                COUNT(*) as review_count
            FROM reviews
            WHERE culture_and_values_rating IS NOT NULL
              AND review_datetime IS NOT NULL
            GROUP BY year, quarter
            ORDER BY year, quarter
        """)
        industry_data = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Format data
        company_trend = []
        for row in company_data:
            company_trend.append({
                'period': f"Q{int(row['quarter'])} {int(row['year'])}",
                'year': int(row['year']),
                'quarter': int(row['quarter']),
                'rating': round(float(row['avg_culture_rating']), 2),
                'review_count': row['review_count']
            })
        
        industry_trend = []
        for row in industry_data:
            industry_trend.append({
                'period': f"Q{int(row['quarter'])} {int(row['year'])}",
                'year': int(row['year']),
                'quarter': int(row['quarter']),
                'rating': round(float(row['avg_culture_rating']), 2),
                'review_count': row['review_count']
            })
        
        return jsonify({
            'success': True,
            'company_name': company_name,
            'company_trend': company_trend,
            'industry_trend': industry_trend
        })
    
    except Exception as e:
        logger.error(f"Error in company culture trend: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/company-culture-score-trend/<company_name>', methods=['GET'])
def get_company_culture_score_trend(company_name):
    """Get yearly culture rating trends for a company - last 5 years
    
    Uses culture_and_values_rating from reviews, normalized against industry average.
    Returns a simplified score: company rating - industry average for each year.
    """
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get company yearly average culture rating
        cursor.execute("""
            SELECT 
                EXTRACT(YEAR FROM review_datetime) as year,
                AVG(culture_and_values_rating) as avg_rating,
                AVG(rating) as avg_overall,
                COUNT(*) as review_count
            FROM reviews
            WHERE company_name = %s 
              AND review_datetime IS NOT NULL
              AND culture_and_values_rating IS NOT NULL
              AND EXTRACT(YEAR FROM review_datetime) >= EXTRACT(YEAR FROM CURRENT_DATE) - 4
            GROUP BY year
            ORDER BY year
        """, (company_name,))
        company_yearly = cursor.fetchall()
        
        # Get industry yearly averages
        cursor.execute("""
            SELECT 
                EXTRACT(YEAR FROM review_datetime) as year,
                AVG(culture_and_values_rating) as avg_rating,
                AVG(rating) as avg_overall
            FROM reviews
            WHERE review_datetime IS NOT NULL
              AND culture_and_values_rating IS NOT NULL
              AND EXTRACT(YEAR FROM review_datetime) >= EXTRACT(YEAR FROM CURRENT_DATE) - 4
            GROUP BY year
            ORDER BY year
        """)
        industry_yearly = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        if not company_yearly:
            return jsonify({
                'success': True,
                'company_name': company_name,
                'trends': []
            })
        
        # Build industry averages lookup
        industry_by_year = {
            int(row['year']): {
                'culture': float(row['avg_rating']) if row['avg_rating'] else 3.0,
                'overall': float(row['avg_overall']) if row['avg_overall'] else 3.0
            }
            for row in industry_yearly
        }
        
        # Calculate normalized scores for each year
        # Score = (company rating - industry average) normalized to roughly match culture score range
        trends = []
        for row in company_yearly:
            year = int(row['year'])
            company_culture = float(row['avg_rating']) if row['avg_rating'] else 3.0
            company_overall = float(row['avg_overall']) if row['avg_overall'] else 3.0
            
            ind = industry_by_year.get(year, {'culture': 3.0, 'overall': 3.0})
            
            # Calculate deviation from industry average, scaled to match culture score range
            # Culture rating is 1-5, so deviation is -4 to +4
            # Scale to roughly -2 to +2 range to match Hofstede/MIT score ranges
            culture_deviation = (company_culture - ind['culture']) * 0.5
            overall_deviation = (company_overall - ind['overall']) * 0.5
            
            # Use culture rating deviation as proxy for Hofstede (soft culture measures)
            # Use overall rating deviation as proxy for MIT (performance-oriented measures)
            # Combined is the average
            hofstede_proxy = round(culture_deviation, 3)
            mit_proxy = round(overall_deviation, 3)
            combined_proxy = round((culture_deviation + overall_deviation) / 2, 3)
            
            trends.append({
                'year': year,
                'hofstede_score': hofstede_proxy,
                'mit_score': mit_proxy,
                'combined_score': combined_proxy,
                'review_count': row['review_count']
            })
        
        return jsonify({
            'success': True,
            'company_name': company_name,
            'trends': trends
        })
    
    except Exception as e:
        logger.error(f"Error in company culture score trend: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/culture-performance-scatter', methods=['GET'])
def get_culture_performance_scatter():
    """Get all companies' culture scores and performance data for scatter plot"""
    try:
        gics_level, gics_value = get_gics_filter_params()
        
        if not performance_analyzer.loaded:
            performance_analyzer.load_data()
        
        company_names = get_companies_for_sector(gics_level=gics_level, gics_value=gics_value)
        
        cached_metrics_map = {}
        conn = get_db_connection()
        if conn and company_names:
            try:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                placeholders = ','.join(['%s'] * len(company_names))
                cursor.execute(f"""
                    SELECT company_name, metrics_json FROM company_metrics_cache
                    WHERE company_name IN ({placeholders})
                """, company_names)
                for row in cursor.fetchall():
                    m = row['metrics_json'] if isinstance(row['metrics_json'], dict) else json.loads(row['metrics_json'])
                    cached_metrics_map[row['company_name']] = m
                cursor.close()
                conn.close()
            except Exception:
                try:
                    conn.close()
                except:
                    pass
        
        hofstede_avg = {dim: [] for dim in HOFSTEDE_DIMENSIONS}
        mit_avg = {dim: [] for dim in MIT_DIMENSIONS}
        all_metrics = {}
        
        uncached_count = 0
        for name in company_names:
            m = cached_metrics_map.get(name)
            if not m:
                uncached_count += 1
                if uncached_count <= 50:
                    m = get_company_metrics(name)
                    if m:
                        cache_metrics(name, m)
            if m:
                all_metrics[name] = m
                for dim in HOFSTEDE_DIMENSIONS:
                    val = m.get('hofstede', {}).get(dim, {}).get('value', 0)
                    hofstede_avg[dim].append(val)
                for dim in MIT_DIMENSIONS:
                    val = m.get('mit_big_9', {}).get(dim, {}).get('value', 0)
                    mit_avg[dim].append(val)
        
        industry_hofstede = {dim: mean(vals) if vals else 0 for dim, vals in hofstede_avg.items()}
        industry_mit = {dim: mean(vals) if vals else 0 for dim, vals in mit_avg.items()}
        
        # Load FMP performance data once (used in both loops below)
        fmp_perf_map = _load_fmp_perf_map()

        # Get correlations for scoring
        culture_data = []
        performance_data = []
        peer_stats = performance_analyzer.get_peer_statistics()
        
        for name in company_names:
            m = all_metrics.get(name)
            if m:
                culture_data.append({
                    'company': name,
                    'hofstede': m.get('hofstede', {}),
                    'mit': m.get('mit_big_9', {})
                })
            perf_metrics = _get_perf_metrics_with_fmp_fallback(name, fmp_perf_map)
            if _has_financial_metrics(perf_metrics):
                perf_metrics['composite_score'] = performance_analyzer.calculate_composite_score(
                    perf_metrics, peer_stats
                )
                performance_data.append(perf_metrics)
        
        correlations = performance_analyzer.calculate_correlation(culture_data, performance_data)
        
        # Extract correlations
        hofstede_correlations = {}
        mit_correlations = {}
        
        hofstede_corr_data = correlations.get('hofstede', {})
        mit_corr_data = correlations.get('mit', {})
        
        for dim in HOFSTEDE_DIMENSIONS:
            dim_data = hofstede_corr_data.get(dim, {}).get('composite_score', {})
            hofstede_correlations[dim] = dim_data.get('correlation', 0) if isinstance(dim_data, dict) else 0
        for dim in MIT_DIMENSIONS:
            dim_data = mit_corr_data.get(dim, {}).get('composite_score', {})
            mit_correlations[dim] = dim_data.get('correlation', 0) if isinstance(dim_data, dict) else 0

        mit_max_values = get_mit_max_values(company_names)
        companies_data = []
        
        for name in company_names:
            metrics = all_metrics.get(name)
            if not metrics:
                continue
            
            # Skip companies with no real culture scores — they'd all cluster at the
            # same combined_score (constant deviation from industry average)
            hofstede_vals = [metrics.get('hofstede', {}).get(d, {}).get('value') or 0 for d in HOFSTEDE_DIMENSIONS]
            mit_vals = [metrics.get('mit_big_9', {}).get(d, {}).get('value') or 0 for d in MIT_DIMENSIONS]
            if all(v == 0 for v in hofstede_vals + mit_vals):
                continue

            perf_metrics = _get_perf_metrics_with_fmp_fallback(name, fmp_perf_map)
            if not _has_financial_metrics(perf_metrics):
                continue
            
            composite_score = performance_analyzer.calculate_composite_score(perf_metrics, peer_stats)
            if composite_score is None:
                continue
            
            # Use business_model already resolved in perf_metrics (includes GICS-based mapping)
            business_model = perf_metrics.get('business_model', 'Unknown')
            if business_model == 'Unknown':
                business_model = 'Traditional'
            
            # Calculate Hofstede company values, score, and weighted confidence
            hofstede_score = 0.0
            hofstede_weighted_conf_sum = 0.0
            hofstede_weight_sum = 0.0
            
            for dim in HOFSTEDE_DIMENSIONS:
                dim_data = metrics.get('hofstede', {}).get(dim, {})
                company_val = dim_data.get('value', 0)
                # Get confidence score (0-100 scale)
                conf_score = dim_data.get('confidence_score', 0) or 0
                conf_normalized = conf_score / 100.0  # Normalize to 0-1
                
                industry_val = industry_hofstede.get(dim, 0)
                correlation = hofstede_correlations.get(dim, 0)
                deviation = company_val - industry_val
                hofstede_score += correlation * deviation
                
                # Weight by |correlation| = sqrt(correlation^2)
                weight = abs(correlation)  # sqrt(r^2) = |r|
                hofstede_weighted_conf_sum += conf_normalized * weight
                hofstede_weight_sum += weight
            
            hofstede_confidence = (hofstede_weighted_conf_sum / hofstede_weight_sum * 100) if hofstede_weight_sum > 0 else 0
            
            # Calculate MIT company values, score, and weighted confidence
            mit_score = 0.0
            mit_weighted_conf_sum = 0.0
            mit_weight_sum = 0.0
            
            for dim in MIT_DIMENSIONS:
                dim_data = metrics.get('mit_big_9', {}).get(dim, {})
                raw_val = dim_data.get('value', 0)
                # Get confidence score (0-100 scale)
                conf_score = dim_data.get('confidence_score', 0) or 0
                conf_normalized = conf_score / 100.0  # Normalize to 0-1
                
                max_val = mit_max_values.get(dim, 1)
                company_val = (10 * (raw_val / max_val)) if max_val > 0 else 0
                
                raw_avg = industry_mit.get(dim, 0)
                industry_val = (10 * (raw_avg / max_val)) if max_val > 0 else 0
                
                correlation = mit_correlations.get(dim, 0)
                deviation = company_val - industry_val
                mit_score += correlation * deviation
                
                # Weight by |correlation| = sqrt(correlation^2)
                weight = abs(correlation)  # sqrt(r^2) = |r|
                mit_weighted_conf_sum += conf_normalized * weight
                mit_weight_sum += weight
            
            mit_confidence = (mit_weighted_conf_sum / mit_weight_sum * 100) if mit_weight_sum > 0 else 0
            
            # Combined score (scale Hofstede to match MIT magnitude)
            combined_score = (hofstede_score * 5) + mit_score
            
            # Combined confidence: weighted average of Hofstede and MIT confidences
            # Weight by total correlation weights from each framework
            total_weight = hofstede_weight_sum + mit_weight_sum
            if total_weight > 0:
                combined_confidence = (
                    (hofstede_confidence * hofstede_weight_sum) + 
                    (mit_confidence * mit_weight_sum)
                ) / total_weight
            else:
                combined_confidence = 0
            
            # Convert to confidence levels
            def get_confidence_level(conf):
                if conf >= 50:
                    return 'High'
                elif conf >= 25:
                    return 'Medium'
                else:
                    return 'Low'
            
            companies_data.append({
                'company_name': name,
                'business_model': business_model,
                'hofstede_score': round(hofstede_score, 3),
                'mit_score': round(mit_score, 3),
                'combined_score': round(combined_score, 3),
                'hofstede_confidence': round(hofstede_confidence, 1),
                'mit_confidence': round(mit_confidence, 1),
                'combined_confidence': round(combined_confidence, 1),
                'hofstede_confidence_level': get_confidence_level(hofstede_confidence),
                'mit_confidence_level': get_confidence_level(mit_confidence),
                'combined_confidence_level': get_confidence_level(combined_confidence),
                'composite_performance': round(composite_score, 1)
            })
        
        return jsonify({
            'success': True,
            'companies': companies_data,
            'business_models': list(set(c['business_model'] for c in companies_data)),
            'sector': gics_value
        })
    
    except Exception as e:
        logger.error(f"Error in culture-performance scatter: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# CSV EXPORT ENDPOINTS
# ============================================================================

@app.route('/api/export/company-reviews/<company_name>')
def export_company_reviews(company_name):
    """Export all reviews for a specific company as CSV download."""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        
        cur = conn.cursor()
        cur.execute("""
            SELECT company_name, review_id, summary, pros, cons, rating, review_link,
                   job_title, employment_status, is_current_employee, years_of_employment,
                   location, advice_to_management,
                   helpful_count, not_helpful_count,
                   business_outlook_rating, career_opportunities_rating, ceo_rating,
                   compensation_and_benefits_rating, culture_and_values_rating,
                   diversity_and_inclusion_rating, recommend_to_friend_rating,
                   senior_management_rating, work_life_balance_rating,
                   language, review_datetime
            FROM reviews
            WHERE company_name = %s
            ORDER BY review_datetime DESC
        """, (company_name,))
        
        rows = cur.fetchall()
        columns = [
            'company_name', 'review_id', 'summary', 'pros', 'cons', 'rating', 'review_link',
            'job_title', 'employment_status', 'is_current_employee', 'years_of_employment',
            'location', 'advice_to_management',
            'helpful_count', 'not_helpful_count',
            'business_outlook_rating', 'career_opportunities_rating', 'ceo_rating',
            'compensation_and_benefits_rating', 'culture_and_values_rating',
            'diversity_and_inclusion_rating', 'recommend_to_friend_rating',
            'senior_management_rating', 'work_life_balance_rating',
            'language', 'review_datetime'
        ]
        
        cur.close()
        conn.close()
        
        import io
        import csv
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(columns)
        for row in rows:
            writer.writerow([str(v) if v is not None else '' for v in row])
        
        safe_name = company_name.replace(' ', '_').replace('&', 'and')
        filename = f"{safe_name}_reviews_{datetime.now().strftime('%Y%m%d')}.csv"
        
        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={filename}'}
        )
    
    except Exception as e:
        logger.error(f"CSV export error for {company_name}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/export/all-reviews')
def export_all_reviews():
    """Export all reviews across all companies as CSV download."""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        
        cur = conn.cursor()
        cur.execute("""
            SELECT company_name, review_id, summary, pros, cons, rating, review_link,
                   job_title, employment_status, is_current_employee, years_of_employment,
                   location, advice_to_management,
                   helpful_count, not_helpful_count,
                   business_outlook_rating, career_opportunities_rating, ceo_rating,
                   compensation_and_benefits_rating, culture_and_values_rating,
                   diversity_and_inclusion_rating, recommend_to_friend_rating,
                   senior_management_rating, work_life_balance_rating,
                   language, review_datetime
            FROM reviews
            ORDER BY company_name, review_datetime DESC
        """)
        
        rows = cur.fetchall()
        columns = [
            'company_name', 'review_id', 'summary', 'pros', 'cons', 'rating', 'review_link',
            'job_title', 'employment_status', 'is_current_employee', 'years_of_employment',
            'location', 'advice_to_management',
            'helpful_count', 'not_helpful_count',
            'business_outlook_rating', 'career_opportunities_rating', 'ceo_rating',
            'compensation_and_benefits_rating', 'culture_and_values_rating',
            'diversity_and_inclusion_rating', 'recommend_to_friend_rating',
            'senior_management_rating', 'work_life_balance_rating',
            'language', 'review_datetime'
        ]
        
        cur.close()
        conn.close()
        
        import io
        import csv
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(columns)
        for row in rows:
            writer.writerow([str(v) if v is not None else '' for v in row])
        
        filename = f"all_reviews_{datetime.now().strftime('%Y%m%d')}.csv"
        
        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={filename}'}
        )
    
    except Exception as e:
        logger.error(f"CSV export error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/export/extraction-summary')
def export_extraction_summary():
    """Export a summary of all companies with extraction status as CSV."""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        
        cur = conn.cursor()
        cur.execute("""
            SELECT c.isin, c.issuer_name, c.company_name, c.company_id, 
                   c.overall_rating, c.review_count,
                   c.total_reviews_extracted, c.gics_sector, c.gics_industry,
                   c.gics_sub_industry, c.country, c.api_source,
                   c.extraction_started, c.extraction_completed,
                   COUNT(r.id) as reviews_in_db
            FROM companies c
            LEFT JOIN reviews r ON c.company_name = r.company_name
            GROUP BY c.isin, c.issuer_name, c.company_name, c.company_id, 
                     c.overall_rating, c.review_count,
                     c.total_reviews_extracted, c.gics_sector, c.gics_industry,
                     c.gics_sub_industry, c.country, c.api_source,
                     c.extraction_started, c.extraction_completed
            ORDER BY c.company_name
        """)
        
        rows = cur.fetchall()
        columns = [
            'isin', 'issuer_name_spreadsheet', 'glassdoor_company_name', 'glassdoor_id',
            'overall_rating', 'review_count_glassdoor',
            'total_extracted', 'gics_sector', 'gics_industry',
            'gics_sub_industry', 'country', 'api_source',
            'extraction_started', 'extraction_completed', 'reviews_in_db'
        ]
        
        cur.close()
        conn.close()
        
        import io
        import csv
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(columns)
        for row in rows:
            writer.writerow([str(v) if v is not None else '' for v in row])
        
        filename = f"extraction_summary_{datetime.now().strftime('%Y%m%d')}.csv"
        
        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={filename}'}
        )
    
    except Exception as e:
        logger.error(f"Summary export error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/export/companies')
def export_companies_list():
    """Get list of companies available for CSV export."""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT c.company_name, c.company_id, c.overall_rating, c.review_count,
                   c.gics_sector, c.api_source,
                   COUNT(r.id) as reviews_in_db
            FROM companies c
            LEFT JOIN reviews r ON c.company_name = r.company_name
            GROUP BY c.company_name, c.company_id, c.overall_rating, c.review_count,
                     c.gics_sector, c.api_source
            ORDER BY c.company_name
        """)
        
        companies = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({'success': True, 'companies': companies})
    
    except Exception as e:
        logger.error(f"Companies list error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/extraction/status')
def extraction_status():
    from extraction_manager import ExtractionManager
    mgr = ExtractionManager.get_instance()
    return jsonify(mgr.get_status())


@app.route('/api/extraction/start', methods=['POST'])
def extraction_start():
    from extraction_manager import ExtractionManager
    mgr = ExtractionManager.get_instance()
    data = request.get_json() or {}
    start_sector = data.get('sector')
    result = mgr.start(start_sector=start_sector)
    return jsonify(result)


@app.route('/api/extraction/pause', methods=['POST'])
def extraction_pause():
    from extraction_manager import ExtractionManager
    mgr = ExtractionManager.get_instance()
    result = mgr.pause()
    return jsonify(result)


@app.route('/api/extraction/stop', methods=['POST'])
def extraction_stop():
    from extraction_manager import ExtractionManager
    mgr = ExtractionManager.get_instance()
    result = mgr.stop()
    return jsonify(result)


@app.route('/api/extraction/sector/<sector>')
def extraction_sector_companies(sector):
    from extraction_manager import ExtractionManager
    mgr = ExtractionManager.get_instance()
    companies = mgr.get_sector_companies(sector)
    return jsonify({'companies': companies, 'sector': sector})


@app.route('/api/extraction/retry/<int:queue_id>', methods=['POST'])
def extraction_retry(queue_id):
    from extraction_manager import ExtractionManager
    mgr = ExtractionManager.get_instance()
    success = mgr.retry_company(queue_id)
    return jsonify({'success': success})


@app.route('/api/extraction/skip/<int:queue_id>', methods=['POST'])
def extraction_skip(queue_id):
    from extraction_manager import ExtractionManager
    mgr = ExtractionManager.get_instance()
    success = mgr.skip_company(queue_id)
    return jsonify({'success': success})


@app.route('/api/extraction/retry-sector/<sector>', methods=['POST'])
def extraction_retry_sector(sector):
    from extraction_manager import ExtractionManager
    data = request.get_json() or {}
    include_wrong_matches = data.get('include_wrong_matches', False)
    mgr = ExtractionManager.get_instance()
    updated = mgr.retry_sector(sector, include_wrong_matches=include_wrong_matches)
    return jsonify({'success': True, 'updated': updated})


@app.route('/api/extraction/retry-all-no-match', methods=['POST'])
def extraction_retry_all_no_match():
    """Reset all no_match (and optionally failed) companies across every sector back to pending."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE extraction_queue
            SET status = 'pending',
                glassdoor_name = NULL,
                glassdoor_id = NULL,
                glassdoor_url = NULL,
                match_confidence = 'none',
                error_message = NULL,
                updated_at = NOW()
            WHERE status IN ('no_match', 'failed')
        """)
        updated = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'success': True, 'updated': updated})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/extraction/update-match/<int:queue_id>', methods=['POST'])
def extraction_update_match(queue_id):
    from extraction_manager import ExtractionManager
    data = request.get_json() or {}
    glassdoor_name = data.get('glassdoor_name')
    glassdoor_id = data.get('glassdoor_id')
    if not glassdoor_name or not glassdoor_id:
        return jsonify({'success': False, 'error': 'glassdoor_name and glassdoor_id required'}), 400
    mgr = ExtractionManager.get_instance()
    success = mgr.update_glassdoor_match(queue_id, glassdoor_name, int(glassdoor_id))
    return jsonify({'success': success})


@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({'success': False, 'error': 'Not found'}), 404


@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    logger.error(f"Internal server error: {error}")
    return jsonify({'success': False, 'error': 'Internal server error'}), 500


# ============================================================================
# APPLICATION ENTRY POINT
# ============================================================================

def init_culture_scores_table():
    """Ensure the review_culture_scores table exists"""
    try:
        conn = get_db_connection()
        if not conn:
            return
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS review_culture_scores (
                review_id INTEGER PRIMARY KEY,
                company_name VARCHAR(255),
                process_results_score REAL,
                job_employee_score REAL,
                professional_parochial_score REAL,
                open_closed_score REAL,
                tight_loose_score REAL,
                pragmatic_normative_score REAL,
                agility_score REAL,
                collaboration_score REAL,
                customer_orientation_score REAL,
                diversity_score REAL,
                execution_score REAL,
                innovation_score REAL,
                integrity_score REAL,
                performance_score REAL,
                respect_score REAL,
                scoring_method VARCHAR(50),
                confidence_level VARCHAR(20),
                scored_at TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("review_culture_scores table verified")
    except Exception as e:
        logger.warning(f"Error initializing culture scores table: {e}")


def ensure_db_indexes():
    """Create database indexes for performance on large tables"""
    try:
        conn = get_db_connection()
        if not conn:
            return
        cursor = conn.cursor()
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_reviews_company_name ON reviews(company_name)",
            "CREATE INDEX IF NOT EXISTS idx_reviews_company_rating ON reviews(company_name, rating)",
            "CREATE INDEX IF NOT EXISTS idx_review_culture_scores_company ON review_culture_scores(company_name)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_review_culture_scores_review_id ON review_culture_scores(review_id)",
            "CREATE INDEX IF NOT EXISTS idx_extraction_queue_status ON extraction_queue(status)",
            "CREATE INDEX IF NOT EXISTS idx_extraction_queue_sector ON extraction_queue(gics_sector)",
        ]
        for idx_sql in indexes:
            try:
                cursor.execute(idx_sql)
            except Exception as e:
                logger.warning(f"Index creation note: {e}")
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Database indexes verified")
    except Exception as e:
        logger.warning(f"Error ensuring indexes: {e}")

def load_excel_performance_data():
    """Load asset management performance data from Excel into fmp_performance_metrics table."""
    try:
        import pandas as pd
        excel_path = 'attached_assets/asset_manager_comprehensive_database_1769351810411.xlsx'
        if not os.path.exists(excel_path):
            logger.warning(f"Excel file not found: {excel_path}")
            return 0
        
        GLASSDOOR_TO_EXCEL = {
            'Goldman Sachs Group': 'Goldman Sachs Group',
            'Fidelity Investments': 'Fidelity Investments',
            'Fidelity International': 'Fidelity International',
            'Franklin Templeton': 'Franklin Templeton',
            'Invesco': 'Invesco',
            'AllianceBernstein': 'AllianceBernstein',
            'Dimensional Fund Advisors': 'Dimensional Fund Advisors',
            'Federated Hermes': 'Federated Hermes',
            'Wellington Management': 'Wellington Management',
            'PIMCO': 'Pimco',
            'Vanguard Group': 'Vanguard Group',
            'Capital Group': 'Capital Group',
            'Robeco': 'Robeco',
            'Natixis Investment Managers': 'Natixis',
            'Nuveen': 'Nuveen',
            'Eurazeo': 'Eurazeo',
        }
        
        aum_df = pd.read_excel(excel_path, 'AUM Data')
        fin_df = pd.read_excel(excel_path, 'Financials & Profitability')
        perf_df = pd.read_excel(excel_path, 'Business Performance')
        tsr_df = pd.read_excel(excel_path, 'Shareholder Returns')
        
        JUNK_PATTERNS = re.compile(
            r'EXPLAINED|METRICS|ROE \(Return|Revenue Yield|Fee-Earning|Operating Margin|Net Margin|NaN|^\s*$',
            re.IGNORECASE
        )
        def is_valid_company(name):
            if not name or not isinstance(name, str):
                return False
            name_str = str(name).strip()
            if not name_str or name_str.lower() == 'nan':
                return False
            return not JUNK_PATTERNS.search(name_str)
        
        excel_data = {}
        for _, row in perf_df.iterrows():
            name = row.get('Company', '')
            if not is_valid_company(name):
                continue
            excel_data[name] = {
                'roe_5y_avg': row.get('5Y Avg ROE (%)'),
            }
        
        for _, row in fin_df.iterrows():
            name = row.get('Company', '')
            if name in excel_data:
                excel_data[name]['op_margin_5y_avg'] = row.get('5Y Avg Op Margin')
                excel_data[name]['revenue_growth_5y'] = row.get('5Y Rev CAGR')
                excel_data[name]['net_margin_latest'] = row.get('2024 Net Margin')
                excel_data[name]['op_margin_latest'] = row.get('2024 Op Margin')
        
        for _, row in tsr_df.iterrows():
            name = row.get('Company', '')
            if name in excel_data:
                excel_data[name]['tsr_cagr_5y'] = row.get('5Y TSR CAGR (%)')
                excel_data[name]['market_cap'] = row.get('2024 Market Cap ($bn)')
                excel_data[name]['ticker'] = row.get('Ticker', '')
        
        for _, row in aum_df.iterrows():
            name = row.get('Company', '')
            if name in excel_data:
                cagr = row.get('5Y CAGR')
                if cagr is not None:
                    try:
                        excel_data[name]['aum_cagr_5y'] = float(cagr) * 100 if abs(float(cagr)) < 1 else float(cagr)
                    except (ValueError, TypeError):
                        pass
        
        conn = get_db_connection()
        if not conn:
            return 0
        
        loaded = 0
        cursor = conn.cursor()
        
        all_mappings = dict(GLASSDOOR_TO_EXCEL)
        for name in excel_data:
            if name not in all_mappings.values():
                all_mappings[name] = name
        
        for glassdoor_name, excel_name in all_mappings.items():
            data = excel_data.get(excel_name)
            if not data:
                continue
            
            roe = data.get('roe_5y_avg')
            op_margin = data.get('op_margin_5y_avg')
            tsr = data.get('tsr_cagr_5y')
            rev_growth = data.get('revenue_growth_5y')
            
            if roe is not None:
                try:
                    roe = float(roe)
                except (ValueError, TypeError):
                    roe = None
            if op_margin is not None:
                try:
                    op_margin = float(op_margin)
                except (ValueError, TypeError):
                    op_margin = None
            if tsr is not None:
                try:
                    tsr = float(tsr)
                except (ValueError, TypeError):
                    tsr = None
            if rev_growth is not None:
                try:
                    rev_growth = float(rev_growth) * 100 if rev_growth and abs(float(rev_growth)) < 1 else float(rev_growth)
                except (ValueError, TypeError):
                    rev_growth = None
            
            market_cap = data.get('market_cap')
            if market_cap is not None:
                try:
                    market_cap = float(market_cap) * 1e9
                except (ValueError, TypeError):
                    market_cap = None
            
            ticker = data.get('ticker', '')
            
            extra_json = {}
            if data.get('aum_cagr_5y') is not None:
                extra_json['aum_cagr_5y'] = data['aum_cagr_5y']
            
            try:
                cursor.execute("""
                    INSERT INTO fmp_performance_metrics
                    (company_name, isin, ticker, gics_sector, gics_industry, gics_sub_industry,
                     roe_latest, roe_5y_avg, op_margin_latest, op_margin_5y_avg,
                     net_margin_latest, revenue_growth_5y, tsr_5y, market_cap,
                     metrics_json, data_source, last_updated)
                    VALUES (%s, NULL, %s, 'Asset Management', 'Asset Management', 'Asset Management',
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, 'excel', NOW())
                    ON CONFLICT (company_name) DO UPDATE SET
                        roe_5y_avg = COALESCE(EXCLUDED.roe_5y_avg, fmp_performance_metrics.roe_5y_avg),
                        op_margin_5y_avg = COALESCE(EXCLUDED.op_margin_5y_avg, fmp_performance_metrics.op_margin_5y_avg),
                        tsr_5y = COALESCE(EXCLUDED.tsr_5y, fmp_performance_metrics.tsr_5y),
                        revenue_growth_5y = COALESCE(EXCLUDED.revenue_growth_5y, fmp_performance_metrics.revenue_growth_5y),
                        market_cap = COALESCE(EXCLUDED.market_cap, fmp_performance_metrics.market_cap),
                        metrics_json = EXCLUDED.metrics_json,
                        data_source = 'excel',
                        gics_sector = 'Asset Management',
                        gics_industry = 'Asset Management',
                        gics_sub_industry = 'Asset Management',
                        last_updated = NOW()
                """, (
                    glassdoor_name, ticker,
                    roe, roe, 
                    float(data.get('op_margin_latest', 0) or 0) if data.get('op_margin_latest') else None,
                    op_margin,
                    float(data.get('net_margin_latest', 0) or 0) if data.get('net_margin_latest') else None,
                    rev_growth, tsr, market_cap,
                    json.dumps(extra_json)
                ))
                loaded += 1
            except Exception as e:
                logger.warning(f"Error loading Excel data for {glassdoor_name}: {e}")
                conn.rollback()
        
        conn.commit()
        # Purge any junk rows that may have slipped through (Excel header rows)
        try:
            cursor2 = conn.cursor()
            cursor2.execute("""
                DELETE FROM fmp_performance_metrics
                WHERE data_source = 'excel'
                  AND (
                    company_name IS NULL
                    OR company_name ~* '(EXPLAINED|METRICS|ROE \(Return|Revenue Yield|Fee-Earning|Operating Margin|Net Margin)'
                    OR LOWER(company_name) = 'nan'
                    OR TRIM(company_name) = ''
                  )
            """)
            purged = cursor2.rowcount
            conn.commit()
            cursor2.close()
            if purged > 0:
                logger.info(f"Purged {purged} junk rows from fmp_performance_metrics")
        except Exception as e:
            logger.warning(f"Junk purge error: {e}")
        cursor.close()
        conn.close()
        logger.info(f"Loaded {loaded} asset management companies from Excel")
        return loaded
    except Exception as e:
        logger.error(f"Error loading Excel performance data: {e}")
        return 0


init_cache_table()
init_extraction_queue()
init_culture_scores_table()
ensure_db_indexes()

from extraction_manager import init_extraction_control
init_extraction_control()
init_fmp_tables()
load_excel_performance_data()

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('FLASK_PORT', os.environ.get('PORT', 8080))))
