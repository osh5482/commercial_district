# config/settings.py
import os
from dotenv import load_dotenv
from .logging import setup_logger

load_dotenv()

API_KEY = os.getenv("API_KEY")

# API 엔드포인트
BASE_URL = "http://apis.data.go.kr/B553077/api/open/sdsc2"

# 로거 초기화
logger = setup_logger()
