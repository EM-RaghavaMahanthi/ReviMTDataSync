import psycopg2
import os
from core.config import settings
import logging

DB_CONNECTION_STRING = settings.DATABASE_URL
SQL_FOLDER = os.path.join(os.path.dirname(__file__), '../sql_cmds')

SQL_FILES_ORDER = [
    #'customers.sql',
    #'membership_instances.sql',
    #'class_sessions.sql',
    #'credit_transactions.sql',
    # 'membership_transactions.sql',
    # 'orders.sql',
    # 'reservations.sql',
    # 'order_lines.sql',
]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("run_sql_cmds")

def run_sql_files_in_order(account_id, location_id):
    import time
    conn = psycopg2.connect(DB_CONNECTION_STRING)
    total_start = time.time()
    try:
        with conn:
            with conn.cursor() as cursor:
                for sql_file in SQL_FILES_ORDER:
                    sql_path = os.path.join(SQL_FOLDER, sql_file)
                    logger.info(f"Running: {sql_file}")
                    file_start = time.time()
                    with open(sql_path, 'r') as f:
                        sql_cmds = f.read()
                    # Replace placeholders
                    sql_cmds = sql_cmds.replace('MY_ACCOUNT_ID', str(account_id))
                    sql_cmds = sql_cmds.replace('MY_LOCATION_ID', str(location_id))
                    try:
                        cursor.execute(sql_cmds)
                        conn.commit()
                        file_end = time.time()
                        logger.info(f"✅ Committed: {sql_file} | ⏱️ Time: {file_end - file_start:.2f} seconds")
                    except Exception as e:
                        file_end = time.time()
                        logger.error(f"❌ Error executing {sql_file}: {e} | ⏱️ Time: {file_end - file_start:.2f} seconds")
                        raise
    finally:
        conn.close()
        total_end = time.time()
        logger.info(f"⏱️ Total time for all SQL files: {total_end - total_start:.2f} seconds")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run SQL files in order with logging and timing.")
    parser.add_argument('--account_id', type=int, default=100, help='Account ID to use in SQL')
    parser.add_argument('--location_id', type=int, default=48718, help='Location ID to use in SQL')
    args = parser.parse_args()
    logger.info(f"Starting SQL file execution for account_id={args.account_id}, location_id={args.location_id}")
    run_sql_files_in_order(account_id=args.account_id, location_id=args.location_id)
