import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")

# API 엔드포인트
BASE_URL = "http://apis.data.go.kr/B553077/api/open/sdsc2"
