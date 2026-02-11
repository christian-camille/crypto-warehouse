import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "crypto_warehouse")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "password")
DB_PORT = os.getenv("DB_PORT", "5432")

def execute_sql_file(cursor, file_path):
    print(f"Executing {file_path}...")
    with open(file_path, 'r') as f:
        sql = f.read()
        cursor.execute(sql)
    print(f"Finished executing {file_path}.")

def setup_database():
    conn = None
    try:
        # Connect to the default postgres database first to check/create the target database
        # Note: This part assumes the user has permission to create databases. 
        print(f"Connecting to database '{DB_NAME}' at {DB_HOST}...")
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Execute DDL
        execute_sql_file(cur, 'sql/01_ddl.sql')
        
        # Execute Procedures
        execute_sql_file(cur, 'sql/02_procedures.sql')
        
        # Pre-populate Dim_Date (e.g., from 2020 to 2030)
        print("Populating Dim_Date...")
        cur.execute("CALL sp_PopulateDateDim('2020-01-01', '2030-12-31');")
        
        # Execute Views
        execute_sql_file(cur, 'sql/03_views.sql')
        
        print("Database setup completed successfully!")
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while setting up database: {error}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    if not os.path.exists('sql/01_ddl.sql'):
        print("Error: Could not find SQL files. Please run this script from the project root.")
    else:
        setup_database()
