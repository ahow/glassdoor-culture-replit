"""
Glassdoor Extraction Orchestrator
Manages parallel extraction of all companies
"""
import os
import json
import logging
import time
import subprocess
from datetime import datetime
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# All 46 companies in order
COMPANIES = [
    # Original 24 companies
    "Sofina", "Brookfield", "Partners Group", "Julius Baer", "UBS Group",
    "HDFC Asset Management", "CVC Capital Partners", "SK Square", "Industrivarden",
    "Eurazeo", "Amundi", "M&G plc", "Schroders", "Ameriprise Financial",
    "Apollo Global Management", "Ares Management", "Blackstone", "BlackRock",
    "Carlyle Group", "Equitable Holdings", "KKR", "Northern Trust",
    "T. Rowe Price", "State Street",
    # Additional 22 companies
    "Robeco", "PIMCO", "Vanguard Group", "Fidelity Investments", "J.P. Morgan Chase",
    "Goldman Sachs Group", "Capital Group", "Allianz Group", "BNY Investments",
    "Invesco", "Legal & General Group", "Franklin Templeton", "Morgan Stanley Inv. Mgmt.",
    "BNP Paribas", "Natixis Investment Managers", "Wellington Mgmt.", "Nuveen",
    "AXA Group", "Federated Hermes", "Dimensional Fund Advisors", "AllianceBernstein",
    "Fidelity International"
]

DATABASE_URL = os.environ.get('DATABASE_URL')

def initialize_database():
    """Initialize database tables"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Create extraction status table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS extraction_status (
                id SERIAL PRIMARY KEY,
                company_name VARCHAR(255) UNIQUE,
                status VARCHAR(50),
                reviews_extracted INTEGER DEFAULT 0,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                error_message TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Initialize status for all companies
        for company in COMPANIES:
            cur.execute("""
                INSERT INTO extraction_status (company_name, status, started_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (company_name) DO NOTHING
            """, (company, 'PENDING', datetime.now()))
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Database initialized with all 46 companies")
        return True
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        return False

def update_status(company_name, status, reviews_extracted=None, error_message=None):
    """Update extraction status in database"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        if status == 'COMPLETED':
            cur.execute("""
                UPDATE extraction_status 
                SET status = %s, reviews_extracted = %s, completed_at = %s, updated_at = NOW()
                WHERE company_name = %s
            """, (status, reviews_extracted, datetime.now(), company_name))
        elif status == 'FAILED':
            cur.execute("""
                UPDATE extraction_status 
                SET status = %s, error_message = %s, completed_at = %s, updated_at = NOW()
                WHERE company_name = %s
            """, (status, error_message, datetime.now(), company_name))
        else:
            cur.execute("""
                UPDATE extraction_status 
                SET status = %s, started_at = %s, updated_at = NOW()
                WHERE company_name = %s
            """, (status, datetime.now(), company_name))
        
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error updating status for {company_name}: {e}")

def extract_company(company_name):
    """Extract reviews for a single company"""
    try:
        logger.info(f"Starting extraction for {company_name}")
        update_status(company_name, 'IN_PROGRESS')
        
        # Run extraction worker
        result = subprocess.run(
            ['python', 'extraction_worker.py', company_name],
            capture_output=True,
            text=True,
            timeout=3600
        )
        
        if result.returncode == 0:
            # Parse output to get review count
            output_lines = result.stdout.split('\n')
            reviews_count = 0
            for line in output_lines:
                if 'reviews extracted' in line.lower():
                    try:
                        reviews_count = int(''.join(filter(str.isdigit, line.split()[-2])))
                    except:
                        pass
            
            update_status(company_name, 'COMPLETED', reviews_extracted=reviews_count)
            logger.info(f"Completed extraction for {company_name}: {reviews_count} reviews")
            return True
        else:
            error_msg = result.stderr[:500]
            update_status(company_name, 'FAILED', error_message=error_msg)
            logger.error(f"Failed extraction for {company_name}: {error_msg}")
            return False
            
    except subprocess.TimeoutExpired:
        error_msg = f"Extraction timeout for {company_name}"
        update_status(company_name, 'FAILED', error_message=error_msg)
        logger.error(error_msg)
        return False
    except Exception as e:
        error_msg = str(e)[:500]
        update_status(company_name, 'FAILED', error_message=error_msg)
        logger.error(f"Error extracting {company_name}: {e}")
        return False

def main():
    """Main orchestrator function"""
    logger.info("Starting Glassdoor extraction orchestrator")
    logger.info(f"Processing {len(COMPANIES)} companies")
    
    # Initialize database
    initialize_database()
    
    # Process companies sequentially (to avoid API rate limits)
    # Can be modified to use ThreadPoolExecutor for parallel processing
    successful = 0
    failed = 0
    
    for company in COMPANIES:
        if extract_company(company):
            successful += 1
        else:
            failed += 1
        
        # Small delay between companies to avoid rate limiting
        time.sleep(5)
    
    logger.info(f"Extraction complete: {successful} successful, {failed} failed")

if __name__ == '__main__':
    main()
