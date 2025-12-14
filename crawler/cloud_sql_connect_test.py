import os
import psycopg2
from google.cloud.sql.connector import Connector, IPTypes


INSTANCE_CONNECTION_NAME = os.environ["INSTANCE_CONNECTION_NAME"]  # e.g., "project:region:instance"
# POSTGRES_HOST = os.getenv("POSTGRES_HOST")
# POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
print(f"{INSTANCE_CONNECTION_NAME=}")
# print(f"{POSTGRES_HOST=}")
# print(f"{POSTGRES_PORT=}")
print(f"{POSTGRES_DB=}")
print(f"{POSTGRES_USER=}")
print(f"{POSTGRES_PASSWORD=}")


print("Initializing Cloud SQL Connector...")
connector = Connector()

def get_db_connection():
    # These environment variables are set in the Cloud Run job configuration.
    # instance_connection_name = os.environ["INSTANCE_CONNECTION_NAME"]  # e.g., "project:region:instance"
    # db_user = os.environ["DB_USER"]                                  # e.g., "my-user"
    # db_pass = os.environ["DB_PASS"]                                  # e.g., from Secret Manager
    # db_name = os.environ["DB_NAME"]                                  # e.g., "my-database"
    
    # Use private IP if the PRIVATE_IP environment variable is set to "true"
    ip_type = IPTypes.PRIVATE if os.environ.get("PRIVATE_IP") == "true" else IPTypes.PUBLIC

    # The connector.connect method returns a database connection object
    return connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        db=POSTGRES_DB,
        ip_type=ip_type
    )

def main_task():
    print("Starting main task...")
    with get_db_connection() as conn:
        print("Database connection established successfully.")
        with conn.cursor() as cursor:
            cursor.execute("SELECT version();")
            print(cursor.fetchone())

# This is the entry point for your Cloud Run Job.
if __name__ == "__main__":
    main_task()

