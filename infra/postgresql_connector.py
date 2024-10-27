import psycopg2
from psycopg2.extras import RealDictCursor

def get_postgres_connection():
    conn = psycopg2.connect(
        dbname="decanaria",
        user="postgres",
        password="123456",
        host="postgres_service",
        port="5432"
    )
    return conn