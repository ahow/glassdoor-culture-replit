"""
Glassdoor Data Extraction via OpenWeb Ninja API
Primary extraction source with RapidAPI fallback.
Saves directly to database AND generates CSV exports for verification.
"""

import os
import sys
import csv
import json
import time
import logging
import requests
from datetime import datetime
import psycopg2
from psycopg2.extras import Json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

OPENWEB_BASE_URL = "https://api.openwebninja.com/realtime-glassdoor-data"
RAPIDAPI_HOST = "real-time-glassdoor-data.p.rapidapi.com"
RAPIDAPI_BASE_URL = f"https://{RAPIDAPI_HOST}"

DATABASE_URL = os.environ.get('DATABASE_URL')

CSV_EXPORT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'exports')

REVIEW_FIELDS = [
    'review_id', 'summary', 'pros', 'cons', 'rating', 'review_link',
    'job_title', 'employment_status', 'is_current_employee', 'years_of_employment',
    'location', 'advice_to_management',
    'helpful_count', 'not_helpful_count',
    'business_outlook_rating', 'career_opportunities_rating', 'ceo_rating',
    'compensation_and_benefits_rating', 'culture_and_values_rating',
    'diversity_and_inclusion_rating', 'recommend_to_friend_rating',
    'senior_management_rating', 'work_life_balance_rating',
    'language', 'review_datetime'
]


def get_db_url():
    db_url = DATABASE_URL
    if db_url and db_url.startswith('postgres://'):
        db_url = db_url.replace('postgres://', 'postgresql://', 1)
    return db_url


def get_db_connection():
    db_url = get_db_url()
    if not db_url:
        raise Exception("DATABASE_URL not set")
    return psycopg2.connect(db_url)


class OpenWebNinjaExtractor:
    """Extracts Glassdoor data using OpenWeb Ninja API (primary) with RapidAPI fallback."""

    def __init__(self, company_name, company_id, glassdoor_url=None, gics_sector=None,
                 gics_industry=None, gics_sub_industry=None, isin=None, country=None,
                 issuer_name=None):
        self.company_name = company_name
        self.company_id = company_id
        self.glassdoor_url = glassdoor_url
        self.gics_sector = gics_sector
        self.gics_industry = gics_industry
        self.gics_sub_industry = gics_sub_industry
        self.isin = isin
        self.country = country
        self.issuer_name = issuer_name

        self.api_source = 'openweb_ninja'
        self.reviews = []
        self.metadata = {}
        self.start_time = datetime.now()
        self.existing_review_ids = set()
        self.pages_extracted = 0
        self.new_reviews_saved = 0

    def _get_openweb_headers(self):
        api_key = os.environ.get('OPENWEB_NINJA_API')
        if not api_key:
            return None
        return {'x-api-key': api_key}

    def _get_rapidapi_headers(self):
        keys = [
            os.environ.get('RAPIDAPI_KEY_1'),
            os.environ.get('RAPIDAPI_KEY_2'),
            os.environ.get('RAPIDAPI_KEY'),
        ]
        keys = [k for k in keys if k]
        if not keys:
            return None
        return {
            'x-rapidapi-key': keys[0],
            'x-rapidapi-host': RAPIDAPI_HOST
        }

    def fetch_reviews_page(self, page=1, sort=None, max_retries=3):
        """Fetch a page of reviews, trying OpenWeb Ninja first, then RapidAPI."""
        params = {'company_id': str(self.company_id), 'page': str(page)}
        if sort:
            params['sort'] = sort

        for attempt in range(max_retries):
            try:
                if self.api_source == 'openweb_ninja':
                    headers = self._get_openweb_headers()
                    if headers:
                        url = f"{OPENWEB_BASE_URL}/company-reviews"
                        response = requests.get(url, headers=headers, params=params, timeout=30)
                        if response.status_code == 200:
                            return response.json()
                        elif response.status_code in (401, 403):
                            logger.warning(f"OpenWeb Ninja auth error ({response.status_code}), falling back to RapidAPI")
                            self.api_source = 'rapidapi_fallback'
                        elif response.status_code == 429:
                            logger.warning("OpenWeb Ninja rate limit, waiting...")
                            time.sleep(5 * (attempt + 1))
                            continue
                        else:
                            logger.warning(f"OpenWeb Ninja error {response.status_code}, attempt {attempt+1}")
                            if attempt < max_retries - 1:
                                time.sleep(2 ** attempt)
                                continue

                if self.api_source in ('rapidapi_fallback', 'rapidapi'):
                    headers = self._get_rapidapi_headers()
                    if headers:
                        url = f"{RAPIDAPI_BASE_URL}/company-reviews"
                        response = requests.get(url, headers=headers, params=params, timeout=30)
                        if response.status_code == 200:
                            return response.json()
                        elif response.status_code == 429:
                            logger.warning("RapidAPI rate limit, waiting...")
                            time.sleep(5 * (attempt + 1))
                            continue
                        else:
                            logger.warning(f"RapidAPI error {response.status_code}")
                            if attempt < max_retries - 1:
                                time.sleep(2 ** attempt)
                                continue

                if attempt == max_retries - 1:
                    raise Exception(f"All API attempts failed for page {page}")

            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on page {page}, attempt {attempt+1}/{max_retries}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error on page {page}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise

        raise Exception(f"Failed to fetch page {page} after {max_retries} retries")

    def search_company(self, query):
        """Search for a company on Glassdoor via the API."""
        headers = self._get_openweb_headers()
        if not headers:
            headers = self._get_rapidapi_headers()
            url = f"{RAPIDAPI_BASE_URL}/company-search"
        else:
            url = f"{OPENWEB_BASE_URL}/company-search"

        if not headers:
            raise Exception("No API keys configured")

        response = requests.get(url, headers=headers, params={'query': query}, timeout=15)
        response.raise_for_status()
        return response.json().get('data', [])

    def get_existing_review_ids(self):
        """Get set of review IDs already in database for this company."""
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT review_id FROM reviews WHERE company_name = %s", (self.company_name,))
            existing = {row[0] for row in cur.fetchall()}
            cur.close()
            conn.close()
            logger.info(f"Found {len(existing)} existing reviews for {self.company_name}")
            return existing
        except Exception as e:
            logger.warning(f"Could not check existing reviews: {e}")
            return set()

    def save_review_batch_to_db(self, reviews_batch):
        """Save a batch of reviews to database immediately."""
        if not reviews_batch:
            return 0

        try:
            conn = get_db_connection()
            cur = conn.cursor()

            saved = 0
            for review in reviews_batch:
                try:
                    cur.execute("SAVEPOINT review_insert")
                    cur.execute("""
                        INSERT INTO reviews 
                        (company_name, review_id, summary, pros, cons, rating, review_link, 
                         job_title, employment_status, is_current_employee, years_of_employment,
                         location, advice_to_management,
                         helpful_count, not_helpful_count, business_outlook_rating, 
                         career_opportunities_rating, ceo_rating, compensation_and_benefits_rating,
                         culture_and_values_rating, diversity_and_inclusion_rating, 
                         recommend_to_friend_rating, senior_management_rating, 
                         work_life_balance_rating, language, review_datetime, review_data)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        review.get('location'),
                        review.get('advice_to_management'),
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
                    cur.execute("RELEASE SAVEPOINT review_insert")
                    saved += 1
                except Exception as e:
                    cur.execute("ROLLBACK TO SAVEPOINT review_insert")
                    logger.warning(f"Error inserting review {review.get('review_id')}: {e}")

            conn.commit()
            cur.close()
            conn.close()
            return saved

        except Exception as e:
            logger.error(f"Database batch save error: {e}")
            return 0

    def save_company_metadata(self):
        """Save company metadata to database."""
        try:
            conn = get_db_connection()
            cur = conn.cursor()

            cur.execute("""
                INSERT INTO companies 
                (company_name, company_id, glassdoor_url, overall_rating, review_count, 
                 page_count, extraction_started, extraction_completed, total_reviews_extracted, 
                 metadata, gics_sector, gics_industry, gics_sub_industry, isin, country, api_source,
                 issuer_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (company_name) DO UPDATE SET
                    total_reviews_extracted = EXCLUDED.total_reviews_extracted,
                    extraction_completed = EXCLUDED.extraction_completed,
                    metadata = EXCLUDED.metadata,
                    gics_sector = COALESCE(EXCLUDED.gics_sector, companies.gics_sector),
                    gics_industry = COALESCE(EXCLUDED.gics_industry, companies.gics_industry),
                    gics_sub_industry = COALESCE(EXCLUDED.gics_sub_industry, companies.gics_sub_industry),
                    isin = COALESCE(EXCLUDED.isin, companies.isin),
                    country = COALESCE(EXCLUDED.country, companies.country),
                    api_source = EXCLUDED.api_source,
                    issuer_name = COALESCE(EXCLUDED.issuer_name, companies.issuer_name)
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
                Json(self.metadata),
                self.gics_sector,
                self.gics_industry,
                self.gics_sub_industry,
                self.isin,
                self.country,
                self.api_source,
                self.issuer_name
            ))

            conn.commit()
            cur.close()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Error saving company metadata: {e}")
            return False

    def extract_all_reviews(self, sort='MOST_RECENT'):
        """Extract all reviews for the company, saving each page to DB immediately."""
        logger.info(f"Starting extraction for {self.company_name} (ID: {self.company_id}) via {self.api_source}")

        self.existing_review_ids = self.get_existing_review_ids()

        try:
            first_page = self.fetch_reviews_page(page=1, sort=sort)
            data = first_page.get('data', {})

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
                'api_source': self.api_source,
            }

            total_pages = self.metadata['page_count']
            logger.info(f"Total reviews: {self.metadata['review_count']} across {total_pages} pages")

            page_reviews = data.get('reviews', [])
            new_reviews = [r for r in page_reviews if r.get('review_id') not in self.existing_review_ids]
            skipped = len(page_reviews) - len(new_reviews)

            if new_reviews:
                saved = self.save_review_batch_to_db(new_reviews)
                self.new_reviews_saved += saved
                self.reviews.extend(new_reviews)
            self.pages_extracted = 1
            logger.info(f"Page 1/{total_pages}: {len(new_reviews)} new, {skipped} existing")

            for page in range(2, total_pages + 1):
                try:
                    page_data = self.fetch_reviews_page(page=page, sort=sort)
                    page_reviews = page_data.get('data', {}).get('reviews', [])

                    if not page_reviews:
                        logger.warning(f"Page {page}/{total_pages}: No reviews returned")
                        continue

                    new_reviews = [r for r in page_reviews if r.get('review_id') not in self.existing_review_ids]
                    skipped = len(page_reviews) - len(new_reviews)

                    if new_reviews:
                        saved = self.save_review_batch_to_db(new_reviews)
                        self.new_reviews_saved += saved
                        self.reviews.extend(new_reviews)

                    self.pages_extracted = page
                    logger.info(f"Page {page}/{total_pages}: {len(new_reviews)} new, {skipped} existing, total saved: {self.new_reviews_saved}")

                    time.sleep(0.5)

                except Exception as e:
                    logger.error(f"Error on page {page}: {e}")
                    logger.error(f"Stopping - saved {self.new_reviews_saved} reviews from {self.pages_extracted} pages")
                    self._log_failure(str(e), page)
                    break

            self.metadata['extraction_completed'] = datetime.now().isoformat()
            self.metadata['total_reviews_extracted'] = self.new_reviews_saved
            self.metadata['existing_reviews_skipped'] = len(self.existing_review_ids)
            self.metadata['pages_extracted'] = self.pages_extracted

            self.save_company_metadata()

            logger.info(f"Completed {self.company_name}: {self.new_reviews_saved} new reviews saved, "
                       f"{len(self.existing_review_ids)} already existed")
            return True

        except Exception as e:
            logger.error(f"Fatal error extracting {self.company_name}: {e}")
            self._log_failure(str(e), None)
            return False

    def _log_failure(self, error_message, page_number):
        """Log extraction failure to database."""
        try:
            conn = get_db_connection()
            cur = conn.cursor()
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
                INSERT INTO extraction_failures 
                (company_name, company_id, error_type, error_message, page_number, reviews_extracted_before_failure)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (self.company_name, self.company_id, 'API_ERROR', error_message,
                  page_number, self.new_reviews_saved))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to log failure: {e}")


def export_company_reviews_csv(company_name, output_path=None):
    """Export all stored reviews for a company as CSV."""
    try:
        conn = get_db_connection()
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

        if not output_path:
            os.makedirs(CSV_EXPORT_DIR, exist_ok=True)
            safe_name = company_name.replace(' ', '_').replace('&', 'and').replace('.', '')
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = os.path.join(CSV_EXPORT_DIR, f"{safe_name}_reviews_{timestamp}.csv")

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(rows)

        logger.info(f"Exported {len(rows)} reviews for {company_name} to {output_path}")
        return output_path, len(rows)

    except Exception as e:
        logger.error(f"CSV export error for {company_name}: {e}")
        return None, 0


def export_all_reviews_csv(output_path=None):
    """Export all reviews across all companies as a single CSV."""
    try:
        conn = get_db_connection()
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

        if not output_path:
            os.makedirs(CSV_EXPORT_DIR, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = os.path.join(CSV_EXPORT_DIR, f"all_reviews_{timestamp}.csv")

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(rows)

        logger.info(f"Exported {len(rows)} total reviews to {output_path}")
        return output_path, len(rows)

    except Exception as e:
        logger.error(f"CSV export error: {e}")
        return None, 0


def export_extraction_summary_csv(output_path=None):
    """Export a summary of all companies with extraction status."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT c.company_name, c.company_id, c.overall_rating, c.review_count,
                   c.total_reviews_extracted, c.gics_sector, c.gics_industry,
                   c.isin, c.country, c.api_source,
                   c.extraction_started, c.extraction_completed,
                   COUNT(r.id) as reviews_in_db
            FROM companies c
            LEFT JOIN reviews r ON c.company_name = r.company_name
            GROUP BY c.company_name, c.company_id, c.overall_rating, c.review_count,
                     c.total_reviews_extracted, c.gics_sector, c.gics_industry,
                     c.isin, c.country, c.api_source,
                     c.extraction_started, c.extraction_completed
            ORDER BY c.company_name
        """)

        rows = cur.fetchall()
        columns = [
            'company_name', 'company_id', 'overall_rating', 'review_count_glassdoor',
            'total_extracted', 'gics_sector', 'gics_industry',
            'isin', 'country', 'api_source',
            'extraction_started', 'extraction_completed', 'reviews_in_db'
        ]

        cur.close()
        conn.close()

        if not output_path:
            os.makedirs(CSV_EXPORT_DIR, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = os.path.join(CSV_EXPORT_DIR, f"extraction_summary_{timestamp}.csv")

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(rows)

        logger.info(f"Exported summary for {len(rows)} companies to {output_path}")
        return output_path, len(rows)

    except Exception as e:
        logger.error(f"Summary export error: {e}")
        return None, 0


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Extract Glassdoor reviews via OpenWeb Ninja API')
    parser.add_argument('company', help='Company name to extract')
    parser.add_argument('--company-id', type=int, required=True, help='Glassdoor company ID')
    parser.add_argument('--sort', default='MOST_RECENT', help='Sort order (default: MOST_RECENT)')
    parser.add_argument('--export-csv', action='store_true', help='Export CSV after extraction')
    parser.add_argument('--sector', help='GICS sector')
    parser.add_argument('--industry', help='GICS industry')

    args = parser.parse_args()

    extractor = OpenWebNinjaExtractor(
        company_name=args.company,
        company_id=args.company_id,
        gics_sector=args.sector,
        gics_industry=args.industry
    )

    success = extractor.extract_all_reviews(sort=args.sort)

    if success and args.export_csv:
        path, count = export_company_reviews_csv(args.company)
        if path:
            logger.info(f"CSV exported: {path} ({count} reviews)")

    sys.exit(0 if success else 1)
