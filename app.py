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
from flask import Flask, render_template, jsonify, request
from datetime import datetime, timedelta
from statistics import mean
from culture_scoring import score_review_with_dictionary

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
        
        for dim in HOFSTEDE_DIMENSIONS:
            if hofstede_avg[dim]:
                avg_val = mean(hofstede_avg[dim])
                hofstede_result[dim] = {'value': round(avg_val, 3), 'confidence': 100, 'confidence_level': 'High'}
        
        # Get max values for MIT rescaling
        mit_max_values = get_mit_max_values()
        
        for dim in MIT_DIMENSIONS:
            if mit_avg[dim]:
                raw_value = mean(mit_avg[dim])
                max_val = mit_max_values.get(dim, 1)
                # Rescale: 10 * (company_value / max_company_value)
                rescaled_value = round(10 * (raw_value / max_val), 2) if max_val > 0 else 0
                mit_result[dim] = {
                    'value': rescaled_value,
                    'raw_value': round(raw_value, 2),
                    'confidence': 100,
                    'confidence_level': 'High'
                }
        
        return jsonify({
            'success': True,
            'company_name': 'Industry Average',
            'hofstede': hofstede_result,
            'mit': mit_result,
            'metadata': {
                'review_count': total_reviews,
                'overall_rating': 0,
                'overall_confidence': 100,
                'overall_confidence_level': 'High',
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
        
        for dim in MIT_DIMENSIONS:
            val1 = profile1.get('mit_big_9', {}).get(dim, {}).get('value', 0)
            val2 = profile2.get('mit_big_9', {}).get(dim, {}).get('value', 0)
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
# ERROR HANDLERS
# ============================================================================

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
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))

# Initialize cache table when app starts (for Gunicorn/production)
init_cache_table()
# Force redeploy
