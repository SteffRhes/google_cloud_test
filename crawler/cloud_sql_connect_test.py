import psycopg
import os


POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
print(f"{POSTGRES_HOST=}")
print(f"{POSTGRES_PORT=}")
print(f"{POSTGRES_DB=}")
print(f"{POSTGRES_USER=}")
print(f"{POSTGRES_PASSWORD=}")


with psycopg.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    ) as conn:

    with conn.cursor() as cur:
        cur.execute("SELECT version();")
        print(cur.fetchone())


