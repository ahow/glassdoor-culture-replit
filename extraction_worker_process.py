"""
Standalone Extraction Worker Process for Heroku.
Runs as a separate 'worker' dyno, independent of the web process.
Auto-resumes extraction after dyno restarts.
Controlled via the extraction_control table in the database.
"""

import os
import sys
import time
import signal
import logging
import psycopg2
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger('extraction_worker')

POLL_INTERVAL = 10
AUTO_RESUME_DELAY = 30
HEARTBEAT_INTERVAL = 60

shutdown_requested = False


def handle_signal(signum, frame):
    global shutdown_requested
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_requested = True


signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)


def get_db_connection():
    db_url = os.environ.get('DATABASE_URL', '')
    if db_url.startswith('postgres://'):
        db_url = db_url.replace('postgres://', 'postgresql://', 1)
    if not db_url:
        raise Exception("DATABASE_URL not set")
    return psycopg2.connect(db_url)


def get_db_command():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT command, current_company, current_sector, updated_at FROM extraction_control WHERE id = 1")
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return {'command': row[0], 'company': row[1], 'sector': row[2], 'updated_at': row[3]}
        return {'command': 'idle', 'company': None, 'sector': None, 'updated_at': None}
    except Exception as e:
        logger.error(f"Error reading extraction_control: {e}")
        return {'command': 'idle', 'company': None, 'sector': None, 'updated_at': None}


def set_db_command(command, current_company=None, current_sector=None):
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


def has_pending_work():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM extraction_queue WHERE status IN ('pending', 'failed')")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count > 0
    except Exception as e:
        logger.error(f"Error checking pending work: {e}")
        return False


def run_extraction(start_sector=None):
    from extraction_manager import ExtractionManager, _set_db_command, init_extraction_control
    init_extraction_control(is_worker=True)

    mgr = ExtractionManager.get_instance()
    logger.info(f"Starting extraction via worker process (sector: {start_sector or 'auto-detect'})")
    _set_db_command('running')
    mgr._run_extraction(start_sector=start_sector)
    logger.info("Extraction run completed")


def find_resume_sector():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        from extraction_manager import SECTOR_ORDER
        for sector in SECTOR_ORDER:
            cur.execute("""
                SELECT COUNT(*) FROM extraction_queue 
                WHERE gics_sector = %s AND status IN ('pending', 'failed')
            """, (sector,))
            pending = cur.fetchone()[0]
            if pending > 0:
                cur.close()
                conn.close()
                return sector
        cur.close()
        conn.close()
        return None
    except Exception as e:
        logger.error(f"Error finding resume sector: {e}")
        return None


def main_loop():
    global shutdown_requested

    logger.info("=" * 60)
    logger.info("Extraction Worker Process started")
    logger.info("=" * 60)

    from extraction_manager import init_extraction_control
    init_extraction_control(is_worker=True)

    logger.info(f"Auto-resume delay: {AUTO_RESUME_DELAY}s, Poll interval: {POLL_INTERVAL}s")
    logger.info(f"Waiting {AUTO_RESUME_DELAY}s before auto-resume check...")
    
    for _ in range(AUTO_RESUME_DELAY):
        if shutdown_requested:
            logger.info("Shutdown requested during startup delay")
            return
        time.sleep(1)

    while not shutdown_requested:
        try:
            ctrl = get_db_command()
            command = ctrl['command']

            if command == 'running':
                if has_pending_work():
                    sector = find_resume_sector()
                    if sector:
                        logger.info(f"Command is 'running' with pending work - starting extraction from sector: {sector}")
                        try:
                            run_extraction(start_sector=sector)
                        except Exception as e:
                            logger.error(f"Extraction error: {e}")
                            set_db_command('idle')
                            time.sleep(30)
                    else:
                        logger.info("Command is 'running' but no pending sectors found - setting idle")
                        set_db_command('idle')
                else:
                    logger.info("Command is 'running' but no pending work - setting idle")
                    set_db_command('idle')

            elif command == 'idle':
                if has_pending_work():
                    logger.info("Auto-resuming: found pending work while idle")
                    sector = find_resume_sector()
                    if sector:
                        logger.info(f"Auto-resuming extraction from sector: {sector}")
                        set_db_command('running', current_sector=sector)
                        try:
                            run_extraction(start_sector=sector)
                        except Exception as e:
                            logger.error(f"Extraction error during auto-resume: {e}")
                            set_db_command('idle')
                            time.sleep(30)

            elif command == 'paused':
                pass

            elif command == 'stop_requested':
                logger.info("Stop requested - setting idle")
                set_db_command('idle')

        except Exception as e:
            logger.error(f"Worker loop error: {e}")

        for i in range(POLL_INTERVAL):
            if shutdown_requested:
                break
            time.sleep(1)

    logger.info("Worker process shutting down gracefully")
    set_db_command('idle')
    logger.info("Extraction Worker Process ended")


if __name__ == '__main__':
    main_loop()
