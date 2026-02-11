# config/settings.py
import os
from dotenv import load_dotenv
from .logging import setup_logger

load_dotenv()

# API 설정
API_KEY = os.getenv("API_KEY")
BASE_URL = "http://apis.data.go.kr/B553077/api/open/sdsc2"

# 데이터베이스 설정
DB_TYPE = os.getenv("DB_TYPE", "sqlite")

# SQLite 설정
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "data/commercial_district.db")

# PostgreSQL 설정
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "commercial_district")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

# PostgreSQL 연결 URL 생성
POSTGRES_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# 로거 초기화
logger = setup_logger()
