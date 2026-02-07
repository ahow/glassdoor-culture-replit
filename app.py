"""
ACWI Glassdoor Dashboard - Production Flask Application
Simplified approach: Query database on-demand instead of pre-loading all data
"""

import os
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

def get_mit_max_values():
    """Get maximum MIT values across all companies for rescaling"""
    global _mit_max_values_cache
    
    # Return cached values if available and recent
    if _mit_max_values_cache:
        return _mit_max_values_cache
    
    try:
        conn = get_db_connection()
        if not conn:
            return {dim: 1 for dim in MIT_DIMENSIONS}  # Fallback
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get max company-level average values for each MIT dimension
        # This calculates AVG per company first, then takes MAX of those averages
        cursor.execute("""
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
                GROUP BY company_name
            ) company_avg
        """)
        
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result:
            _mit_max_values_cache = {
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
            _mit_max_values_cache = {dim: 1 for dim in MIT_DIMENSIONS}
        
        return _mit_max_values_cache
        
    except Exception as e:
        logger.error(f"Error getting MIT max values: {e}")
        return {dim: 1 for dim in MIT_DIMENSIONS}

def get_company_metrics(company_name):
    """Get aggregated metrics for a company from the database"""
    try:
        conn = get_db_connection()
        if not conn:
            return None
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get all reviews for this company
        cursor.execute("""
            SELECT * FROM reviews 
            WHERE company_name = %s
            ORDER BY review_datetime DESC
        """, (company_name,))
        
        reviews = cursor.fetchall()
        review_count = len(reviews)
        
        if review_count == 0:
            cursor.close()
            conn.close()
            return None
        
        # Extract and aggregate ratings
        ratings = [r['rating'] for r in reviews if r['rating']]
        wlb_ratings = [r['work_life_balance_rating'] for r in reviews if r['work_life_balance_rating']]
        culture_ratings = [r['culture_and_values_rating'] for r in reviews if r['culture_and_values_rating']]
        career_ratings = [r['career_opportunities_rating'] for r in reviews if r['career_opportunities_rating']]
        comp_ratings = [r['compensation_and_benefits_rating'] for r in reviews if r['compensation_and_benefits_rating']]
        mgmt_ratings = [r['senior_management_rating'] for r in reviews if r['senior_management_rating']]
        
        # Parse recommend and CEO ratings from review_data JSON
        recommend_ratings = []
        ceo_ratings = []
        
        for review in reviews:
            if review['review_data']:
                try:
                    data = review['review_data'] if isinstance(review['review_data'], dict) else json.loads(review['review_data'])
                    if data.get('recommend_to_friend_rating'):
                        try:
                            recommend_ratings.append(float(data['recommend_to_friend_rating']))
                        except (ValueError, TypeError):
                            pass
                    if data.get('ceo_rating'):
                        try:
                            ceo_ratings.append(float(data['ceo_rating']))
                        except (ValueError, TypeError):
                            pass
                except:
                    pass
        
        # Use pre-calculated culture scores from review_culture_scores table
        # This is much faster than scoring each review fresh
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
        
        # Map database columns to dimension names
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
        
        # Build Hofstede metrics from pre-calculated aggregates
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
        
        # Build MIT metrics from pre-calculated aggregates
        # Store raw values - rescaling to 0-10 scale is done in the API response
        mit_avg = {}
        if culture_result and scored_review_count > 0:
            for db_col, dim in mit_dim_map.items():
                value = culture_result.get(db_col)
                count = culture_result.get(f'{db_col}_count', 0)
                if value is not None and count > 0:
                    # Store raw value - rescaling happens in API response
                    mit_avg[dim] = {
                        'value': round(float(value), 4),  # Raw value
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
            'overall_rating': round(mean(ratings), 2) if ratings else 0,
            'culture_values': round(mean(culture_ratings), 2) if culture_ratings else 0,
            'work_life_balance': round(mean(wlb_ratings), 2) if wlb_ratings else 0,
            'career_opportunities': round(mean(career_ratings), 2) if career_ratings else 0,
            'compensation_benefits': round(mean(comp_ratings), 2) if comp_ratings else 0,
            'senior_management': round(mean(mgmt_ratings), 2) if mgmt_ratings else 0,
            'recommend_percentage': round((sum(1 for r in recommend_ratings if r and r >= 4) / len(recommend_ratings) * 100), 1) if recommend_ratings else 0,
            'ceo_approval': round(mean(ceo_ratings), 2) if ceo_ratings else 0,
            'hofstede': hofstede_avg,
            'mit_big_9': mit_avg
        }
        
        # Calculate relative confidence scores based on evidence
        metrics = calculate_relative_confidence(metrics)
        
        # Debug logging
        logger.info(f"Metrics for {company_name} after confidence calc:")
        logger.info(f"  Hofstede sample: {list(metrics['hofstede'].items())[0] if metrics['hofstede'] else 'empty'}")
        logger.info(f"  MIT sample: {list(metrics['mit_big_9'].items())[0] if metrics['mit_big_9'] else 'empty'}")
        
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


@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get overall dashboard statistics"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get overall statistics
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT company_name) as total_companies,
                COUNT(*) as total_reviews,
                AVG(rating) as average_rating
            FROM reviews
        """)
        
        stats = cursor.fetchone()
        
        # Get all companies with their metrics
        cursor.execute("""
            SELECT DISTINCT company_name FROM reviews
            ORDER BY company_name
        """)
        
        company_names = [row['company_name'] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        
        # Get metrics for each company (with caching)
        companies = []
        for company_name in company_names:
            # Try to get from cache first
            metrics = get_cached_metrics(company_name)
            
            # If not in cache, calculate and cache it
            if not metrics:
                metrics = get_company_metrics(company_name)
                if metrics:
                    cache_metrics(company_name, metrics)
            
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
                    'industry': ''
                })
        
        return jsonify({
            'success': True,
            'total_companies': stats['total_companies'] or 0,
            'total_reviews': stats['total_reviews'] or 0,
            'avg_rating': round(float(stats['average_rating']), 2) if stats['average_rating'] else 0,
            'companies': companies
        })
    
    except Exception as e:
        logger.error(f"Error in get_stats: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/companies', methods=['GET'])
def get_companies():
    """Get all companies with their metrics"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get all companies
        cursor.execute("""
            SELECT DISTINCT company_name FROM reviews
            ORDER BY company_name
        """)
        
        company_names = [row['company_name'] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        
        # Get metrics for each company (with caching)
        companies = []
        for company_name in company_names:
            # Try to get from cache first
            metrics = get_cached_metrics(company_name)
            
            # If not in cache, calculate and cache it
            if not metrics:
                metrics = get_company_metrics(company_name)
                if metrics:
                    cache_metrics(company_name, metrics)
            
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
                    'industry': ''
                })
        
        # Calculate overall statistics
        all_ratings = [c['overall_rating'] for c in companies if c['overall_rating']]
        avg_rating = round(mean(all_ratings), 2) if all_ratings else 0
        
        return jsonify({
            'success': True,
            'companies': companies,
            'avg_rating': avg_rating
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
        
        # Get max values for MIT rescaling
        mit_max_values = get_mit_max_values()
        
        mit_response = {}
        for dim, data in metrics['mit_big_9'].items():
            raw_value = data.get('value', 0) or 0
            max_val = mit_max_values.get(dim, 1)
            # Rescale: 10 * (company_value / max_company_value)
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


@app.route('/api/industry-average', methods=['GET'])
def get_industry_average():
    """Get industry average culture profile"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT DISTINCT company_name FROM reviews ORDER BY company_name")
        company_names = [row['company_name'] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        
        hofstede_avg = {dim: [] for dim in HOFSTEDE_DIMENSIONS}
        mit_avg = {dim: [] for dim in MIT_DIMENSIONS}
        total_reviews = 0
        
        for company_name in company_names:
            metrics = get_cached_metrics(company_name)
            if not metrics:
                metrics = get_company_metrics(company_name)
                if metrics:
                    cache_metrics(company_name, metrics)
            
            if metrics:
                total_reviews += metrics.get('total_reviews', 0)
                for dim in HOFSTEDE_DIMENSIONS:
                    val = metrics.get('hofstede', {}).get(dim, {}).get('value', 0)
                    hofstede_avg[dim].append(val)
                for dim in MIT_DIMENSIONS:
                    val = metrics.get('mit_big_9', {}).get(dim, {}).get('value', 0)
                    mit_avg[dim].append(val)
        
        hofstede_result = {}
        mit_result = {}
        
        # Calculate max evidence across all dimensions for relative confidence
        all_company_metrics = []
        for company_name in company_names:
            metrics = get_cached_metrics(company_name)
            if not metrics:
                metrics = get_company_metrics(company_name)
            if metrics:
                all_company_metrics.append(metrics)
        
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
        
        # Get max values for MIT rescaling
        mit_max_values = get_mit_max_values()
        
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
            'company_name': 'Industry Average',
            'hofstede': hofstede_result,
            'mit': mit_result,
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
    """Get list of all companies for dropdown menus"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get all unique company names sorted alphabetically
        cursor.execute("""
            SELECT DISTINCT company_name FROM reviews
            ORDER BY company_name
        """)
        
        companies = [row['company_name'] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'companies': companies
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
    """Get benchmarking data comparing company to industry averages"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get company profile
        company_profile = get_company_metrics(company_name)
        if not company_profile:
            return jsonify({'success': False, 'error': 'Company not found'}), 404
        
        # Get all companies for benchmarking
        cursor.execute("""
            SELECT DISTINCT company_name FROM reviews
            WHERE company_name != %s
            ORDER BY company_name
        """, (company_name,))
        
        other_companies = [row['company_name'] for row in cursor.fetchall()]
        
        # Calculate industry averages
        hofstede_avg = {dim: [] for dim in HOFSTEDE_DIMENSIONS}
        mit_avg = {dim: [] for dim in MIT_DIMENSIONS}
        
        for other_company in other_companies:
            other_profile = get_company_metrics(other_company)
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
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'company': company_name,
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

@app.route('/api/performance-correlation', methods=['GET'])
def get_performance_correlation():
    """Get correlation analysis between culture metrics and business performance"""
    try:
        # Load performance data if not already loaded
        if not performance_analyzer.loaded:
            performance_analyzer.load_data()
        
        # Get all companies with culture data
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute('SELECT DISTINCT company_name FROM reviews ORDER BY company_name')
        culture_companies = [row['company_name'] for row in cursor.fetchall()]
        conn.close()
        
        # Collect culture and performance data for each company
        culture_data = []
        performance_data = []
        
        peer_stats = performance_analyzer.get_peer_statistics()
        
        for company in culture_companies:
            # Get culture metrics
            metrics = get_cached_metrics(company)
            if not metrics:
                metrics = get_company_metrics(company)
            
            if metrics:
                culture_data.append({
                    'company': company,
                    'hofstede': metrics.get('hofstede', {}),
                    'mit': metrics.get('mit_big_9', {})
                })
            
            # Get performance metrics
            perf_metrics = performance_analyzer.get_performance_metrics(company)
            if perf_metrics and len(perf_metrics) > 2:
                # Calculate composite score
                perf_metrics['composite_score'] = performance_analyzer.calculate_composite_score(
                    perf_metrics, peer_stats
                )
                performance_data.append(perf_metrics)
        
        # Calculate correlations
        correlations = performance_analyzer.calculate_correlation(culture_data, performance_data)
        
        return jsonify({
            'success': True,
            'correlations': correlations,
            'companies_with_both': len([p for p in performance_data if p.get('composite_score')]),
            'culture_companies': len(culture_companies),
            'performance_companies': len(performance_data)
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
        
        perf_metrics = performance_analyzer.get_performance_metrics(company_name)
        
        if not perf_metrics or len(perf_metrics) <= 2:
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
        if not performance_analyzer.loaded:
            performance_analyzer.load_data()
        
        # Get all companies with culture data
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute('SELECT DISTINCT company_name FROM reviews ORDER BY company_name')
        culture_companies = [row['company_name'] for row in cursor.fetchall()]
        conn.close()
        
        rankings = []
        peer_stats = performance_analyzer.get_peer_statistics()
        
        for company in culture_companies:
            perf_metrics = performance_analyzer.get_performance_metrics(company)
            if perf_metrics and len(perf_metrics) > 2:
                composite = performance_analyzer.calculate_composite_score(perf_metrics, peer_stats)
                if composite is not None:
                    # Get culture metrics
                    culture_metrics = get_cached_metrics(company)
                    if not culture_metrics:
                        culture_metrics = get_company_metrics(company)
                    
                    rankings.append({
                        'company': company,
                        'composite_score': round(composite, 1),
                        'business_model': perf_metrics.get('business_model', 'Unknown'),
                        'roe_5y_avg': perf_metrics.get('roe_5y_avg'),
                        'aum_cagr_5y': round(perf_metrics.get('aum_cagr_5y', 0) * 100, 1) if perf_metrics.get('aum_cagr_5y') else None,
                        'tsr_cagr_5y': perf_metrics.get('tsr_cagr_5y'),
                        'culture_confidence': culture_metrics.get('overall_confidence', 0) if culture_metrics else 0
                    })
        
        # Sort by composite score descending
        rankings.sort(key=lambda x: x['composite_score'], reverse=True)
        
        # Add rank
        for i, r in enumerate(rankings):
            r['rank'] = i + 1
        
        return jsonify({
            'success': True,
            'rankings': rankings,
            'total': len(rankings)
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
        # Get company culture profile
        metrics = get_cached_metrics(company_name)
        if not metrics:
            metrics = get_company_metrics(company_name)
            if metrics:
                cache_metrics(company_name, metrics)
        
        if not metrics:
            return jsonify({'success': False, 'error': 'Company not found'}), 404
        
        # Get industry averages
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT DISTINCT company_name FROM reviews ORDER BY company_name")
        company_names = [row['company_name'] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        
        hofstede_avg = {dim: [] for dim in HOFSTEDE_DIMENSIONS}
        mit_avg = {dim: [] for dim in MIT_DIMENSIONS}
        
        for name in company_names:
            m = get_cached_metrics(name)
            if not m:
                m = get_company_metrics(name)
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
        
        mit_max_values = get_mit_max_values()
        for dim in MIT_DIMENSIONS:
            if mit_avg[dim]:
                raw_avg = mean(mit_avg[dim])
                max_val = mit_max_values.get(dim, 1)
                industry_mit[dim] = round(10 * (raw_avg / max_val), 2) if max_val > 0 else 0
        
        # Get correlations with composite score
        if not performance_analyzer.loaded:
            performance_analyzer.load_data()
        
        culture_data = []
        performance_data = []
        peer_stats = performance_analyzer.get_peer_statistics()
        
        for name in company_names:
            m = get_cached_metrics(name)
            if not m:
                m = get_company_metrics(name)
            if m:
                culture_data.append({
                    'company': name,
                    'hofstede': m.get('hofstede', {}),
                    'mit': m.get('mit_big_9', {})
                })
            perf_metrics = performance_analyzer.get_performance_metrics(name)
            if perf_metrics and len(perf_metrics) > 2:
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
        # Ensure performance data is loaded
        if not performance_analyzer.loaded:
            performance_analyzer.load_data()
        
        # Get all company names
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT DISTINCT company_name FROM reviews ORDER BY company_name")
        company_names = [row['company_name'] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        
        # Calculate industry averages first
        hofstede_avg = {dim: [] for dim in HOFSTEDE_DIMENSIONS}
        mit_avg = {dim: [] for dim in MIT_DIMENSIONS}
        all_metrics = {}
        
        for name in company_names:
            m = get_cached_metrics(name)
            if not m:
                m = get_company_metrics(name)
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
            perf_metrics = performance_analyzer.get_performance_metrics(name)
            if perf_metrics and len(perf_metrics) > 2:
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
        
        # Calculate culture scores for each company
        mit_max_values = get_mit_max_values()
        companies_data = []
        
        for name in company_names:
            metrics = all_metrics.get(name)
            if not metrics:
                continue
            
            # Get performance data
            perf_metrics = performance_analyzer.get_performance_metrics(name)
            if not perf_metrics or len(perf_metrics) <= 2:
                continue
            
            composite_score = performance_analyzer.calculate_composite_score(perf_metrics, peer_stats)
            if composite_score is None:
                continue
            
            # Get business model category
            business_model = performance_analyzer.get_business_model(name)
            
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
            'business_models': list(set(c['business_model'] for c in companies_data))
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

if __name__ == '__main__':
    # Initialize cache table on startup
    init_cache_table()
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('FLASK_PORT', os.environ.get('PORT', 8080))))

# Initialize cache table when app starts (for Gunicorn/production)
init_cache_table()
# Force redeploy
