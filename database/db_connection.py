# Script : db_connection.py
# Location : scripts/db_connection.py
#
# Provides a single reusable function to get a PostgreSQL connection.
# All other scripts import from here instead of duplicating connection logic.
#
# Prerequisites:
#   pip install psycopg2-binary python-dotenv

import os
import psycopg2
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root, forcing latin-1 encoding for Windows compatibility
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(env_path, encoding="latin-1")


def get_connection():
    # Returns an open psycopg2 connection using parameters from .env
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "fc_metz"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD"),
    )