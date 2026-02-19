from sqlalchemy import create_engine
import psycopg2

# Database credentials
DB_USER = "postgres"
DB_PASSWORD = "12345"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "retail_dw"

# SQLAlchemy engine
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

def get_db_connection():
    """
    Create and return PostgreSQL database connection using psycopg2
    """
    try:
        connection = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME
        )
        return connection
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise
