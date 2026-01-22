"""
Clean up duplicate reviews in the database
Keeps only the first occurrence of each (company_name, review_id) pair
"""

import os
import psycopg2
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

def cleanup_duplicates():
    """Remove duplicate reviews, keeping only the first occurrence"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        logger.info("Starting duplicate cleanup...")
        
        # First, check how many duplicates exist
        cur.execute("""
            SELECT COUNT(*) - COUNT(DISTINCT (company_name, review_id))
            FROM reviews
        """)
        duplicate_count = cur.fetchone()[0]
        logger.info(f"Found {duplicate_count} duplicate reviews")
        
        if duplicate_count == 0:
            logger.info("No duplicates to clean up!")
            return True
        
        # Delete duplicates, keeping only the row with the smallest id for each (company_name, review_id)
        cur.execute("""
            DELETE FROM reviews
            WHERE id NOT IN (
                SELECT MIN(id)
                FROM reviews
                GROUP BY company_name, review_id
            )
        """)
        
        deleted_count = cur.rowcount
        conn.commit()
        
        logger.info(f"Successfully deleted {deleted_count} duplicate reviews")
        
        # Verify cleanup
        cur.execute("SELECT COUNT(*) FROM reviews")
        total_reviews = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(DISTINCT (company_name, review_id)) FROM reviews")
        unique_reviews = cur.fetchone()[0]
        
        logger.info(f"After cleanup: {total_reviews} total reviews, {unique_reviews} unique reviews")
        
        if total_reviews == unique_reviews:
            logger.info("✓ All duplicates successfully removed!")
        else:
            logger.warning(f"⚠ Still have {total_reviews - unique_reviews} duplicates")
        
        cur.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        return False

if __name__ == '__main__':
    success = cleanup_duplicates()
    exit(0 if success else 1)
