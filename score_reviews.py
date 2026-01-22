"""
Phase 3: Score all reviews and aggregate to company profiles
This script processes all reviews and creates company culture profiles
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from urllib.parse import urlparse
from culture_scoring import score_review_with_dictionary, aggregate_review_scores
import statistics
from datetime import datetime

def get_db_connection():
    """Get database connection"""
    DATABASE_URL = os.environ.get('DATABASE_URL')
    if DATABASE_URL and DATABASE_URL.startswith('postgres://'):
        DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def score_all_reviews():
    """Score all reviews in the database"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    print("Fetching all reviews...")
    cur.execute("""
        SELECT id, company_name, summary, pros, cons, review_datetime
        FROM reviews
        ORDER BY company_name, review_datetime
    """)
    
    reviews = cur.fetchall()
    print(f"Found {len(reviews)} reviews to score")
    
    # Score each review
    scored_count = 0
    for i, review in enumerate(reviews):
        if i % 1000 == 0:
            print(f"Scoring review {i+1}/{len(reviews)}...")
        
        # Combine text fields for scoring
        review_text = f"{review['summary'] or ''} {review['pros'] or ''} {review['cons'] or ''}"
        
        if not review_text.strip():
            continue
        
        # Score the review
        scores = score_review_with_dictionary(review_text)
        
        if not scores:
            continue
        
        # Insert into database
        try:
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
                review['id'],
                review['company_name'],
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
            scored_count += 1
            
            if scored_count % 5000 == 0:
                conn.commit()
                print(f"  Committed {scored_count} scores")
        
        except Exception as e:
            print(f"Error scoring review {review['id']}: {e}")
            continue
    
    conn.commit()
    print(f"\n✅ Scored {scored_count} reviews")
    cur.close()
    conn.close()

def aggregate_to_company_profiles():
    """Aggregate review scores to company-level profiles"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    print("\nAggregating scores to company profiles...")
    
    # Get list of companies
    cur.execute("SELECT DISTINCT company_name FROM reviews ORDER BY company_name")
    companies = [row['company_name'] for row in cur.fetchall()]
    
    print(f"Processing {len(companies)} companies...")
    
    for company in companies:
        # Get all scores for this company
        cur.execute("""
            SELECT 
                process_results_score, job_employee_score, professional_parochial_score,
                open_closed_score, tight_loose_score, pragmatic_normative_score,
                agility_score, collaboration_score, customer_orientation_score,
                diversity_score, execution_score, innovation_score, integrity_score,
                performance_score, respect_score
            FROM review_culture_scores
            WHERE company_name = %s AND process_results_score IS NOT NULL
        """, (company,))
        
        scores = cur.fetchall()
        
        if not scores:
            print(f"  ⚠️  No scores for {company}")
            continue
        
        # Calculate aggregates
        hofstede_dims = [
            'process_results_score', 'job_employee_score', 'professional_parochial_score',
            'open_closed_score', 'tight_loose_score', 'pragmatic_normative_score'
        ]
        
        mit_dims = [
            'agility_score', 'collaboration_score', 'customer_orientation_score',
            'diversity_score', 'execution_score', 'innovation_score', 'integrity_score',
            'performance_score', 'respect_score'
        ]
        
        aggregates = {}
        
        # Hofstede aggregates
        for dim in hofstede_dims:
            values = [s[dim] for s in scores if s[dim] is not None]
            if values:
                aggregates[f'{dim}_mean'] = statistics.mean(values)
                aggregates[f'{dim}_std'] = statistics.stdev(values) if len(values) > 1 else 0
        
        # MIT Big 9 aggregates
        for dim in mit_dims:
            values = [s[dim] for s in scores if s[dim] is not None]
            if values:
                aggregates[f'{dim}_mean'] = statistics.mean(values)
                aggregates[f'{dim}_std'] = statistics.stdev(values) if len(values) > 1 else 0
        
        # Get date range
        cur.execute("""
            SELECT MIN(review_datetime) as min_date, MAX(review_datetime) as max_date
            FROM reviews
            WHERE company_name = %s
        """, (company,))
        
        date_row = cur.fetchone()
        min_date = date_row['min_date'] if date_row else None
        max_date = date_row['max_date'] if date_row else None
        
        # Insert or update company profile
        try:
            cur.execute("""
                INSERT INTO company_culture_profiles
                (company_name, 
                 process_results_mean, process_results_std,
                 job_employee_mean, job_employee_std,
                 professional_parochial_mean, professional_parochial_std,
                 open_closed_mean, open_closed_std,
                 tight_loose_mean, tight_loose_std,
                 pragmatic_normative_mean, pragmatic_normative_std,
                 agility_mean, agility_std,
                 collaboration_mean, collaboration_std,
                 customer_orientation_mean, customer_orientation_std,
                 diversity_mean, diversity_std,
                 execution_mean, execution_std,
                 innovation_mean, innovation_std,
                 integrity_mean, integrity_std,
                 performance_mean, performance_std,
                 respect_mean, respect_std,
                 review_count, last_updated, data_period_start, data_period_end)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (company_name) DO UPDATE SET
                 process_results_mean = EXCLUDED.process_results_mean,
                 process_results_std = EXCLUDED.process_results_std,
                 job_employee_mean = EXCLUDED.job_employee_mean,
                 job_employee_std = EXCLUDED.job_employee_std,
                 professional_parochial_mean = EXCLUDED.professional_parochial_mean,
                 professional_parochial_std = EXCLUDED.professional_parochial_std,
                 open_closed_mean = EXCLUDED.open_closed_mean,
                 open_closed_std = EXCLUDED.open_closed_std,
                 tight_loose_mean = EXCLUDED.tight_loose_mean,
                 tight_loose_std = EXCLUDED.tight_loose_std,
                 pragmatic_normative_mean = EXCLUDED.pragmatic_normative_mean,
                 pragmatic_normative_std = EXCLUDED.pragmatic_normative_std,
                 agility_mean = EXCLUDED.agility_mean,
                 agility_std = EXCLUDED.agility_std,
                 collaboration_mean = EXCLUDED.collaboration_mean,
                 collaboration_std = EXCLUDED.collaboration_std,
                 customer_orientation_mean = EXCLUDED.customer_orientation_mean,
                 customer_orientation_std = EXCLUDED.customer_orientation_std,
                 diversity_mean = EXCLUDED.diversity_mean,
                 diversity_std = EXCLUDED.diversity_std,
                 execution_mean = EXCLUDED.execution_mean,
                 execution_std = EXCLUDED.execution_std,
                 innovation_mean = EXCLUDED.innovation_mean,
                 innovation_std = EXCLUDED.innovation_std,
                 integrity_mean = EXCLUDED.integrity_mean,
                 integrity_std = EXCLUDED.integrity_std,
                 performance_mean = EXCLUDED.performance_mean,
                 performance_std = EXCLUDED.performance_std,
                 respect_mean = EXCLUDED.respect_mean,
                 respect_std = EXCLUDED.respect_std,
                 review_count = EXCLUDED.review_count,
                 last_updated = EXCLUDED.last_updated,
                 data_period_start = EXCLUDED.data_period_start,
                 data_period_end = EXCLUDED.data_period_end
            """, (
                company,
                aggregates.get('process_results_score_mean'),
                aggregates.get('process_results_score_std'),
                aggregates.get('job_employee_score_mean'),
                aggregates.get('job_employee_score_std'),
                aggregates.get('professional_parochial_score_mean'),
                aggregates.get('professional_parochial_score_std'),
                aggregates.get('open_closed_score_mean'),
                aggregates.get('open_closed_score_std'),
                aggregates.get('tight_loose_score_mean'),
                aggregates.get('tight_loose_score_std'),
                aggregates.get('pragmatic_normative_score_mean'),
                aggregates.get('pragmatic_normative_score_std'),
                aggregates.get('agility_score_mean'),
                aggregates.get('agility_score_std'),
                aggregates.get('collaboration_score_mean'),
                aggregates.get('collaboration_score_std'),
                aggregates.get('customer_orientation_score_mean'),
                aggregates.get('customer_orientation_score_std'),
                aggregates.get('diversity_score_mean'),
                aggregates.get('diversity_score_std'),
                aggregates.get('execution_score_mean'),
                aggregates.get('execution_score_std'),
                aggregates.get('innovation_score_mean'),
                aggregates.get('innovation_score_std'),
                aggregates.get('integrity_score_mean'),
                aggregates.get('integrity_score_std'),
                aggregates.get('performance_score_mean'),
                aggregates.get('performance_score_std'),
                aggregates.get('respect_score_mean'),
                aggregates.get('respect_score_std'),
                len(scores),
                datetime.now(),
                min_date,
                max_date
            ))
            
            print(f"  ✅ {company}: {len(scores)} reviews aggregated")
        
        except Exception as e:
            print(f"  ❌ Error aggregating {company}: {e}")
    
    conn.commit()
    print("\n✅ All company profiles created!")
    cur.close()
    conn.close()

if __name__ == '__main__':
    print("=" * 70)
    print("PHASE 3: Score All Reviews and Aggregate to Company Profiles")
    print("=" * 70)
    
    score_all_reviews()
    aggregate_to_company_profiles()
    
    print("\n" + "=" * 70)
    print("✅ PHASE 3 COMPLETE!")
    print("=" * 70)
