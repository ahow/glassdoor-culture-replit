"""
Glassdoor Data Extraction Worker
Extracts all reviews for a single company from Glassdoor API
Designed to run in parallel on Heroku
"""

import os
import sys
import json
import time
import logging
import requests
from datetime import datetime
import psycopg2
from psycopg2.extras import Json
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# API Configuration - Multiple keys for increased bandwidth
API_KEYS = [
    os.environ.get('RAPIDAPI_KEY_1'),
    os.environ.get('RAPIDAPI_KEY_2'),
    os.environ.get('RAPIDAPI_KEY'),  # Fallback to original key name
]
API_KEYS = [k for k in API_KEYS if k]  # Filter out None values
API_KEY_INDEX = 0  # Current key index for rotation
API_HOST = "real-time-glassdoor-data.p.rapidapi.com"
API_BASE_URL = f"https://{API_HOST}"

def get_api_key():
    """Get the current API key, rotating through available keys"""
    global API_KEY_INDEX
    if not API_KEYS:
        raise Exception("No RAPIDAPI keys configured. Set RAPIDAPI_KEY_1 and RAPIDAPI_KEY_2")
    key = API_KEYS[API_KEY_INDEX % len(API_KEYS)]
    API_KEY_INDEX += 1
    return key

# Database Configuration
DATABASE_URL = os.environ.get('DATABASE_URL')

# Company IDs mapping
COMPANY_IDS = {
    "Sofina": 1814225,
    "Brookfield": 5824,
    "Partners Group": 308505,
    "Julius Baer": 12799,
    "UBS Group": 3419,
    "HDFC Asset Management": 513311,
    "CVC Capital Partners": 9415,
    "SK Square": 8443114,
    "Industrivarden": 40145,
    "Eurazeo": 10328,
    "Amundi": 316188,
    "M&G plc": 36436,
    "Schroders": 10512,
    "Ameriprise Financial": 15316,
    "Apollo Global Management": 2715,
    "Ares Management": 35082,
    "Blackstone": 4022,
    "BlackRock": 9331,
    "Carlyle Group": 3670,
    "Equitable Holdings": 19955,
    "KKR": 2865,
    "Northern Trust": 1710,
    "T. Rowe Price": 3583,
    "State Street": 1911,
    # Additional Asset Management Companies
    "Robeco": 214902,
    "PIMCO": 3585,
    "Vanguard Group": 4084,
    "Fidelity Investments": 2786,
    "J.P. Morgan Chase": 145,
    "Goldman Sachs Group": 2800,
    "Capital Group": 9441,
    "Allianz Group": 3062,
    "BNY Investments": 78,
    "Invesco": 4518,
    "Legal & General Group": 10189,
    "Franklin Templeton": 240744,
    "Morgan Stanley Inv. Mgmt.": 2282,
    "BNP Paribas": 3140805,
    "Natixis Investment Managers": 10682,
    "Wellington Mgmt.": 9606,
    "Nuveen": 3563,
    "AXA Group": 3137236,
    "Federated Hermes": 4556053,
    "Dimensional Fund Advisors": 29863,
    "AllianceBernstein": 14976,
    "Fidelity International": 636122,
}

GLASSDOOR_URLS = {
    "Sofina": "https://www.glassdoor.com/Reviews/Sofina-Reviews-E1814225.htm",
    "Brookfield": "https://www.glassdoor.com/Reviews/Brookfield-Reviews-E5824.htm",
    "Partners Group": "https://www.glassdoor.com/Reviews/Partners-Group-Reviews-E308505.htm",
    "Julius Baer": "https://www.glassdoor.com/Reviews/Julius-Baer-Reviews-E12799.htm",
    "UBS Group": "https://www.glassdoor.com/Reviews/UBS-Reviews-E3419.htm",
    "HDFC Asset Management": "https://www.glassdoor.com/Reviews/HDFC-Asset-Management-Company-Reviews-E513311.htm",
    "CVC Capital Partners": "https://www.glassdoor.com/Reviews/CVC-Capital-Partners-Reviews-E9415.htm",
    "SK Square": "https://www.glassdoor.com/Overview/Working-at-SK-Square-EI_IE8443114.11,20.htm",
    "Industrivarden": "https://www.glassdoor.co.uk/Overview/Working-at-AB-Industriv%C3%A4rden-EI_IE40145.11,28.htm",
    "Eurazeo": "https://www.glassdoor.com/Reviews/Eurazeo-Reviews-E10328.htm",
    "Amundi": "https://www.glassdoor.com/Reviews/Amundi-Reviews-E316188.htm",
    "M&G plc": "https://www.glassdoor.com/Reviews/M-and-G-plc-Reviews-E36436.htm",
    "Schroders": "https://www.glassdoor.com/Reviews/Schroders-Reviews-E10512.htm",
    "Ameriprise Financial": "https://www.glassdoor.com/Reviews/Ameriprise-Reviews-E15316.htm",
    "Apollo Global Management": "https://www.glassdoor.com/Reviews/Apollo-Global-Management-Reviews-E2715.htm",
    "Ares Management": "https://www.glassdoor.com/Reviews/Ares-Management-Reviews-E35082.htm",
    "Blackstone": "https://www.glassdoor.com/Reviews/The-Blackstone-Group-Reviews-E4022.htm",
    "BlackRock": "https://www.glassdoor.com/Reviews/BlackRock-Reviews-E9331.htm",
    "Carlyle Group": "https://www.glassdoor.com/Reviews/The-Carlyle-Group-Reviews-E3670.htm",
    "Equitable Holdings": "https://www.glassdoor.com/Reviews/Equitable-Reviews-E19955.htm",
    "KKR": "https://www.glassdoor.com/Reviews/KKR-Reviews-E2865.htm",
    "Northern Trust": "https://www.glassdoor.com/Reviews/Northern-Trust-Reviews-E1710.htm",
    "T. Rowe Price": "https://www.glassdoor.com/Reviews/T-Rowe-Price-Reviews-E3583.htm",
    "State Street": "https://www.glassdoor.com/Reviews/State-Street-Reviews-E1911.htm",
    # Additional Asset Management Companies
    "Robeco": "https://www.glassdoor.co.uk/Reviews/Robeco-Reviews-E214902.htm",
    "PIMCO": "https://www.glassdoor.co.uk/Reviews/PIMCO-Reviews-E3585.htm",
    "Vanguard Group": "https://www.glassdoor.co.uk/Reviews/Vanguard-Reviews-E4084.htm",
    "Fidelity Investments": "https://www.glassdoor.co.uk/Reviews/Fidelity-Investments-Reviews-E2786.htm",
    "J.P. Morgan Chase": "https://www.glassdoor.co.uk/Reviews/J-P-Morgan-asset-management-Reviews-EI_IE145.0,10_KO11,27.htm",
    "Goldman Sachs Group": "https://www.glassdoor.co.uk/Reviews/Goldman-Sachs-Asset-Management-Reviews-EI_IE2800.0,13_KO14,30.htm",
    "Capital Group": "https://www.glassdoor.co.uk/Overview/Working-at-Capital-Group-EI_IE9441.11,24.htm",
    "Allianz Group": "https://www.glassdoor.co.uk/Reviews/Allianz-Reviews-E3062.htm",
    "BNY Investments": "https://www.glassdoor.co.uk/Reviews/BNY-Investment-Management-Reviews-EI_IE78.0,3_KO4,25.htm",
    "Invesco": "https://www.glassdoor.co.uk/Reviews/Invesco-Reviews-E4518.htm",
    "Legal & General Group": "https://www.glassdoor.co.uk/Overview/Working-at-Legal-and-General-EI_IE10189.11,28.htm",
    "Franklin Templeton": "https://www.glassdoor.co.uk/Reviews/Franklin-Templeton-Reviews-E240744.htm",
    "Morgan Stanley Inv. Mgmt.": "https://www.glassdoor.co.uk/Reviews/Morgan-Stanley-Investment-Management-Reviews-EI_IE2282.0,14_KO15,36.htm",
    "BNP Paribas": "https://www.glassdoor.co.uk/Reviews/BNP-Paribas-Asset-Management-Reviews-E3140805.htm",
    "Natixis Investment Managers": "https://www.glassdoor.co.uk/Reviews/Natixis-Reviews-E10682.htm",
    "Wellington Mgmt.": "https://www.glassdoor.co.uk/Reviews/Wellington-Management-Reviews-E9606.htm",
    "Nuveen": "https://www.glassdoor.co.uk/Overview/Working-at-Nuveen-EI_IE3563.11,17.htm",
    "AXA Group": "https://www.glassdoor.co.uk/Reviews/AXA-Investment-Managers-Reviews-E3137236.htm",
    "Federated Hermes": "https://www.glassdoor.co.uk/Reviews/Federated-Hermes-Reviews-E4556053.htm",
    "Dimensional Fund Advisors": "https://www.glassdoor.co.uk/Overview/Working-at-Dimensional-Fund-Advisors-EI_IE29863.11,36.htm",
    "AllianceBernstein": "https://www.glassdoor.co.uk/Reviews/AllianceBernstein-Reviews-E14976.htm",
    "Fidelity International": "https://www.glassdoor.co.uk/Reviews/Fidelity-International-Reviews-E636122.htm",
}


class GlassdoorExtractor:
    def __init__(self, company_name):
        self.company_name = company_name
        self.company_id = COMPANY_IDS.get(company_name)
        self.glassdoor_url = GLASSDOOR_URLS.get(company_name)
        
        if not self.company_id:
            raise ValueError(f"Unknown company: {company_name}")
        
        self.headers = {
            "x-rapidapi-host": API_HOST
        }
        
        self.reviews = []
        self.metadata = {}
        self.start_time = datetime.now()
        self.existing_review_ids = set()  # Track reviews already in database
    
    def get_company_reviews(self, page=1, max_retries=3):
        """Fetch reviews for a specific page"""
        url = f"{API_BASE_URL}/company-reviews"
        params = {
            "company_id": str(self.company_id),
            "page": str(page)
        }
        
        for attempt in range(max_retries):
            try:
                # Get fresh API key for each request (rotation for increased bandwidth)
                request_headers = {**self.headers, "x-rapidapi-key": get_api_key()}
                response = requests.get(url, headers=request_headers, params=params, timeout=30)
                
                # Check for API errors (401, 429, etc.)
                if response.status_code == 401:
                    logger.error(f"API Authentication error (401) - Invalid API key")
                    raise Exception("API Authentication failed - check RAPIDAPI_KEY")
                elif response.status_code == 429:
                    logger.error(f"API Rate limit exceeded (429)")
                    raise Exception("API Rate limit exceeded - wait before retrying")
                elif response.status_code == 403:
                    logger.error(f"API Forbidden (403) - Access denied")
                    raise Exception("API Access forbidden")
                
                response.raise_for_status()
                
                # Validate JSON response
                json_data = response.json()
                if not json_data or 'data' not in json_data:
                    logger.error(f"Invalid API response format for page {page}: {json_data}")
                    raise Exception(f"Invalid API response format")
                
                return json_data
                
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on page {page}, attempt {attempt + 1}/{max_retries}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error on page {page}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise
    
    def get_existing_review_ids(self):
        """Get set of review IDs already in database for this company"""
        try:
            if not DATABASE_URL:
                return set()
            
            db_url = DATABASE_URL
            if db_url.startswith('postgres://'):
                db_url = db_url.replace('postgres://', 'postgresql://', 1)
            
            conn = psycopg2.connect(db_url)
            cur = conn.cursor()
            
            cur.execute("""
                SELECT review_id FROM reviews 
                WHERE company_name = %s
            """, (self.company_name,))
            
            existing_ids = {row[0] for row in cur.fetchall()}
            
            cur.close()
            conn.close()
            
            logger.info(f"Found {len(existing_ids)} existing reviews for {self.company_name} in database")
            return existing_ids
            
        except Exception as e:
            logger.warning(f"Could not check existing reviews: {e}")
            return set()
    
    def extract_all_reviews(self):
        """Extract all reviews for the company"""
        logger.info(f"Starting extraction for {self.company_name} (ID: {self.company_id})")
        
        # Get existing review IDs to skip API calls for already-extracted reviews
        self.existing_review_ids = self.get_existing_review_ids()
        
        try:
            # Get first page to determine total pages
            first_page_data = self.get_company_reviews(page=1)
            data = first_page_data.get('data', {})
            
            # Store metadata
            self.metadata = {
                'company_name': self.company_name,
                'company_id': self.company_id,
                'glassdoor_url': self.glassdoor_url,
                'review_count': data.get('review_count', 0),
                'page_count': data.get('page_count', 0),
                'filtered_review_count': data.get('filtered_review_count', 0),
                'rated_review_count': data.get('rated_review_count', 0),
                'overall_rating': data.get('rating', 0),
                'extraction_started': self.start_time.isoformat(),
            }
            
            logger.info(f"Total reviews for {self.company_name}: {self.metadata['review_count']} "
                       f"across {self.metadata['page_count']} pages")
            
            # Extract reviews from first page, filtering out existing ones
            if 'reviews' in data:
                new_reviews = [r for r in data['reviews'] if r.get('review_id') not in self.existing_review_ids]
                skipped = len(data['reviews']) - len(new_reviews)
                self.reviews.extend(new_reviews)
                logger.info(f"Page 1: {len(new_reviews)} new reviews, {skipped} already in database")
            
            # Extract remaining pages
            total_pages = self.metadata['page_count']
            for page in range(2, total_pages + 1):
                try:
                    page_data = self.get_company_reviews(page=page)
                    reviews = page_data.get('data', {}).get('reviews', [])
                    
                    if reviews:
                        # Filter out reviews that already exist in database
                        new_reviews = [r for r in reviews if r.get('review_id') not in self.existing_review_ids]
                        skipped = len(reviews) - len(new_reviews)
                        self.reviews.extend(new_reviews)
                        logger.info(f"Page {page}/{total_pages}: {len(new_reviews)} new reviews, {skipped} already in database")
                    else:
                        logger.warning(f"Page {page}/{total_pages}: No reviews returned")
                    
                    # Rate limiting
                    time.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"FATAL: Error extracting page {page} for {self.company_name}: {e}")
                    logger.error(f"Stopping extraction for {self.company_name} - extracted {len(self.reviews)} reviews from {page-1} pages")
                    
                    # Log failure to database
                    error_type = "API_ERROR"
                    if "401" in str(e) or "Authentication" in str(e):
                        error_type = "API_AUTH_ERROR"
                    elif "429" in str(e) or "Rate limit" in str(e):
                        error_type = "API_RATE_LIMIT"
                    elif "403" in str(e):
                        error_type = "API_FORBIDDEN"
                    
                    self.log_extraction_failure(error_type, str(e), page)
                    
                    # Stop extraction on error instead of continuing
                    break
            
            new_count = len(self.reviews)
            existing_count = len(self.existing_review_ids)
            total_in_db = existing_count + new_count
            
            logger.info(f"Completed extraction for {self.company_name}: "
                       f"{new_count} new reviews extracted, "
                       f"{existing_count} already in database, "
                       f"{total_in_db} total")
            
            self.metadata['extraction_completed'] = datetime.now().isoformat()
            self.metadata['total_reviews_extracted'] = len(self.reviews)
            self.metadata['existing_reviews_skipped'] = existing_count
            
            return True
            
        except Exception as e:
            logger.error(f"Fatal error extracting {self.company_name}: {e}")
            
            # Log fatal failure to database
            error_type = "FATAL_ERROR"
            if "401" in str(e) or "Authentication" in str(e):
                error_type = "API_AUTH_ERROR"
            elif "429" in str(e) or "Rate limit" in str(e):
                error_type = "API_RATE_LIMIT"
            
            self.log_extraction_failure(error_type, str(e), None)
            
            return False
    
    def log_extraction_failure(self, error_type, error_message, page_number=None):
        """Log extraction failure to database for tracking and retry"""
        try:
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()
            
            # Create table if it doesn't exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS extraction_failures (
                    id SERIAL PRIMARY KEY,
                    company_name VARCHAR(255),
                    company_id INTEGER,
                    error_type VARCHAR(100),
                    error_message TEXT,
                    page_number INTEGER,
                    reviews_extracted_before_failure INTEGER,
                    timestamp TIMESTAMP DEFAULT NOW(),
                    retry_attempted BOOLEAN DEFAULT FALSE,
                    retry_successful BOOLEAN DEFAULT NULL
                )
            """)
            
            # Insert failure record
            cur.execute("""
                INSERT INTO extraction_failures 
                (company_name, company_id, error_type, error_message, page_number, reviews_extracted_before_failure)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                self.company_name,
                self.company_id,
                error_type,
                error_message,
                page_number,
                len(self.reviews)
            ))
            
            conn.commit()
            cur.close()
            conn.close()
            
            logger.info(f"Logged extraction failure for {self.company_name} to database")
            return True
            
        except Exception as e:
            logger.error(f"Failed to log extraction failure to database: {e}")
            return False
    
    def save_to_database(self):
        """Save extracted reviews to PostgreSQL database"""
        # Validate data before saving
        if not self.reviews:
            logger.warning(f"No reviews to save for {self.company_name}")
            return True  # Not an error, just no new reviews
        
        if not self.metadata:
            logger.error(f"No metadata available for {self.company_name} - cannot save")
            return False
        
        # Validate reviews have required fields
        valid_reviews = []
        for review in self.reviews:
            if review.get('review_id') and (review.get('summary') or review.get('pros') or review.get('cons')):
                valid_reviews.append(review)
            else:
                logger.warning(f"Skipping invalid review (missing review_id or content): {review.get('review_id')}")
        
        if not valid_reviews:
            logger.error(f"No valid reviews to save for {self.company_name}")
            return False
        
        logger.info(f"Saving {len(valid_reviews)} valid reviews for {self.company_name}")
        self.reviews = valid_reviews  # Replace with validated reviews
        
        try:
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()
            
            # Create tables if they don't exist
            # Create extraction_failures table to track all failed attempts
            cur.execute("""
                CREATE TABLE IF NOT EXISTS extraction_failures (
                    id SERIAL PRIMARY KEY,
                    company_name VARCHAR(255),
                    company_id INTEGER,
                    error_type VARCHAR(100),
                    error_message TEXT,
                    page_number INTEGER,
                    reviews_extracted_before_failure INTEGER,
                    timestamp TIMESTAMP DEFAULT NOW(),
                    retry_attempted BOOLEAN DEFAULT FALSE,
                    retry_successful BOOLEAN DEFAULT NULL
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS companies (
                    id SERIAL PRIMARY KEY,
                    company_name VARCHAR(255) UNIQUE,
                    company_id INTEGER,
                    glassdoor_url TEXT,
                    overall_rating FLOAT,
                    review_count INTEGER,
                    page_count INTEGER,
                    extraction_started TIMESTAMP,
                    extraction_completed TIMESTAMP,
                    total_reviews_extracted INTEGER,
                    metadata JSONB,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS reviews (
                    id SERIAL PRIMARY KEY,
                    company_name VARCHAR(255),
                    review_id INTEGER,
                    summary TEXT,
                    pros TEXT,
                    cons TEXT,
                    rating INTEGER,
                    review_link TEXT,
                    job_title VARCHAR(255),
                    employment_status VARCHAR(50),
                    is_current_employee BOOLEAN,
                    years_of_employment INTEGER,
                    helpful_count INTEGER,
                    not_helpful_count INTEGER,
                    business_outlook_rating VARCHAR(50),
                    career_opportunities_rating INTEGER,
                    ceo_rating VARCHAR(50),
                    compensation_and_benefits_rating INTEGER,
                    culture_and_values_rating INTEGER,
                    diversity_and_inclusion_rating INTEGER,
                    recommend_to_friend_rating VARCHAR(50),
                    senior_management_rating INTEGER,
                    work_life_balance_rating INTEGER,
                    language VARCHAR(10),
                    review_datetime TIMESTAMP,
                    review_data JSONB,
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(company_name, review_id)
                )
            """)
            
            # Insert company metadata
            cur.execute("""
                INSERT INTO companies 
                (company_name, company_id, glassdoor_url, overall_rating, review_count, 
                 page_count, extraction_started, extraction_completed, total_reviews_extracted, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (company_name) DO UPDATE SET
                    total_reviews_extracted = EXCLUDED.total_reviews_extracted,
                    extraction_completed = EXCLUDED.extraction_completed,
                    metadata = EXCLUDED.metadata
            """, (
                self.company_name,
                self.company_id,
                self.glassdoor_url,
                self.metadata.get('overall_rating'),
                self.metadata.get('review_count'),
                self.metadata.get('page_count'),
                self.metadata.get('extraction_started'),
                self.metadata.get('extraction_completed'),
                self.metadata.get('total_reviews_extracted'),
                Json(self.metadata)
            ))
            
            # Insert reviews
            for review in self.reviews:
                cur.execute("""
                    INSERT INTO reviews 
                    (company_name, review_id, summary, pros, cons, rating, review_link, 
                     job_title, employment_status, is_current_employee, years_of_employment,
                     helpful_count, not_helpful_count, business_outlook_rating, 
                     career_opportunities_rating, ceo_rating, compensation_and_benefits_rating,
                     culture_and_values_rating, diversity_and_inclusion_rating, 
                     recommend_to_friend_rating, senior_management_rating, 
                     work_life_balance_rating, language, review_datetime, review_data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (company_name, review_id) DO NOTHING
                """, (
                    self.company_name,
                    review.get('review_id'),
                    review.get('summary'),
                    review.get('pros'),
                    review.get('cons'),
                    review.get('rating'),
                    review.get('review_link'),
                    review.get('job_title'),
                    review.get('employment_status'),
                    review.get('is_current_employee'),
                    review.get('years_of_employment'),
                    review.get('helpful_count'),
                    review.get('not_helpful_count'),
                    review.get('business_outlook_rating'),
                    review.get('career_opportunities_rating'),
                    review.get('ceo_rating'),
                    review.get('compensation_and_benefits_rating'),
                    review.get('culture_and_values_rating'),
                    review.get('diversity_and_inclusion_rating'),
                    review.get('recommend_to_friend_rating'),
                    review.get('senior_management_rating'),
                    review.get('work_life_balance_rating'),
                    review.get('language'),
                    review.get('review_datetime'),
                    Json(review)
                ))
            
            conn.commit()
            cur.close()
            conn.close()
            
            logger.info(f"Successfully saved {len(self.reviews)} reviews to database for {self.company_name}")
            return True
            
        except Exception as e:
            logger.error(f"Database error for {self.company_name}: {e}")
            return False
    
    def save_to_json(self, output_dir="/tmp"):
        """Save extracted reviews to JSON file"""
        try:
            output_file = os.path.join(output_dir, f"{self.company_name.replace(' ', '_')}_reviews.json")
            
            data = {
                'metadata': self.metadata,
                'reviews': self.reviews
            }
            
            with open(output_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            logger.info(f"Saved {len(self.reviews)} reviews to {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving to JSON: {e}")
            return False


def main():
    parser = argparse.ArgumentParser(description='Extract Glassdoor reviews for a company')
    parser.add_argument('company', help='Company name to extract')
    parser.add_argument('--output-dir', default='/tmp', help='Output directory for JSON files')
    parser.add_argument('--db-only', action='store_true', help='Only save to database, not JSON')
    
    args = parser.parse_args()
    
    try:
        extractor = GlassdoorExtractor(args.company)
        
        # Extract reviews
        if not extractor.extract_all_reviews():
            logger.error(f"Failed to extract reviews for {args.company}")
            sys.exit(1)
        
        # Save to database
        if not extractor.save_to_database():
            logger.error(f"Failed to save to database for {args.company}")
            sys.exit(1)
        
        # Save to JSON (unless --db-only flag)
        if not args.db_only:
            if not extractor.save_to_json(args.output_dir):
                logger.error(f"Failed to save to JSON for {args.company}")
                sys.exit(1)
        
        logger.info(f"Successfully completed extraction for {args.company}")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
