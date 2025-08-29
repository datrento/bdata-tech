import os
from sqlalchemy import create_engine

def get_engine():
    db_url = os.getenv(
        "POSTGRES_URL",
        "postgresql+psycopg2://postgres:postgres@postgres:5432/price_intelligence",
    )
    return create_engine(db_url)


