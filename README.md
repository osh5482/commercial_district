# 🏪 Commercial District Analysis

전국 상권 데이터 수집 및 분석 파이프라인

## 📊 프로젝트 개요

공공데이터포털의 소상공인시장진흥공단 상가(상권)정보 API를 활용하여
**데이터 수집 → 전처리 → 저장 → 시각화**의 전체 파이프라인을 구현한 데이터 분석 프로젝트입니다.

## 🎯 주요 기능

### ✅ 구현 완료 (Phase 1 - MVP)

#### 1. 데이터 수집 파이프라인
- **비동기 API 클라이언트** (`aiohttp` 기반)
  - 상가업소 정보 (`/stores`)
  - 행정구역 정보 (`/district`)
  - 상권 정보 (`/store_zone`)
  - 업종 분류 정보 (`/upjong`)
- **배치 수집**: 대용량 데이터 효율적 수집
- **에러 핸들링 및 재시도 로직**
- **상세 로깅 시스템**

#### 2. 데이터 저장
- **다중 포맷 지원**: CSV, Parquet
- **SQLite 데이터베이스**: 약 58MB 규모
- **전처리 파이프라인**: 결측치 처리, 데이터 타입 변환

#### 3. 인터랙티브 대시보드 (Streamlit)
- **다중 필터링**
  - 시군구/행정동 선택 (계층적 필터링)
  - 업종 대분류/중분류 선택
  - 상호명 키워드 검색
- **주요 지표 (KPI)**
  - 총 점포 수
  - 시군구/행정동 수
  - 업종 중분류 수
  - 좌표 보유율
- **시각화**
  - 업종별 점포 수 Top 10 (막대 차트)
  - 행정동별 점포 수 Top 10 (막대 차트)
  - 상가업소 밀집도 히트맵 (Folium)
- **데이터 테이블**: 필터링된 결과 확인

### 🔜 예정 (Phase 2 이후)

- **자동화 파이프라인** (Apache Airflow)
- **클라우드 스토리지** (S3 / MinIO)
- **PostgreSQL + PostGIS** (공간 데이터 분석)
- **대용량 처리** (PySpark)
- **고급 지도 시각화** (Deck.gl)
- **ML 기반 상권 분석** (클러스터링, 예측 모델)

## 🛠️ 기술 스택

### 현재 사용 중
- **언어**: Python 3.12.4
- **데이터 수집**: `aiohttp`, `asyncio`
- **데이터 처리**: `pandas`, `numpy`
- **데이터 저장**: SQLite, Parquet
- **시각화**: Streamlit, Plotly, Folium
- **환경 관리**: Anaconda

### 향후 도입 예정
- Apache Airflow, Docker
- PostgreSQL, PostGIS
- PySpark
- AWS S3 / GCP Storage

## 📁 프로젝트 구조

```
commercial_district/
├── config/                  # 설정 파일
│   ├── settings.py         # 환경 변수, API 키
│   └── logging.py          # 로깅 설정
├── src/                    # 소스 코드
│   ├── clients/           # API 클라이언트
│   │   ├── base.py       # 기본 클라이언트 클래스
│   │   ├── store.py      # 상가업소 API
│   │   ├── district.py   # 행정구역 API
│   │   ├── store_zone.py # 상권 API
│   │   └── upjong.py     # 업종 분류 API
│   ├── collector.py       # 데이터 수집 오케스트레이터
│   ├── preprocessor.py    # 데이터 전처리
│   ├── storage.py         # 파일 저장 (CSV, Parquet)
│   └── database.py        # SQLite 데이터베이스 관리
├── data/                   # 데이터 디렉토리
│   ├── raw/               # 원본 데이터 (CSV, Parquet)
│   ├── processed/         # 전처리된 데이터
│   └── commercial_district.db  # SQLite DB
├── logs/                   # 로그 파일
├── main.py                # 데이터 수집 실행 스크립트
├── streamlit_app.py       # 대시보드 애플리케이션
├── environment.yml        # Conda 환경 설정
└── .env                   # API 키 (git 제외)
```

## 🚀 설치 및 실행

### 1. 환경 설정

```bash
# Conda 환경 생성 및 활성화
conda env create -f environment.yml
conda activate commercial_district
```

### 2. API 키 설정

`.env` 파일을 생성하고 API 키를 입력하세요:

```env
API_KEY=your_api_key_here
```

> API 키는 [공공데이터포털](https://www.data.go.kr/data/15012005/openapi.do)에서 발급받을 수 있습니다.

### 3. 데이터 수집

```bash
# 데이터 수집 실행
python main.py
```

수집된 데이터는 다음 위치에 저장됩니다:
- `data/raw/`: 원본 CSV/Parquet 파일
- `data/commercial_district.db`: SQLite 데이터베이스

### 4. 대시보드 실행

```bash
# Streamlit 대시보드 실행
streamlit run streamlit_app.py
```

브라우저에서 `http://localhost:8501`로 접속하면 대시보드를 확인할 수 있습니다.

## 📊 대시보드 미리보기

> 스크린샷 예정

## 📚 데이터 출처

- **소상공인시장진흥공단 상가(상권)정보 API**
  - [공공데이터포털 링크](https://www.data.go.kr/data/15012005/openapi.do)
  - 전국 상가업소 정보, 상권 정보, 업종 분류 제공

## 🎓 학습 목표

이 프로젝트를 통해 다음을 경험하고 있습니다:

1. **데이터 엔지니어링**
   - 공공 API 연동 및 대용량 데이터 수집
   - 비동기 프로그래밍 (`asyncio`, `aiohttp`)
   - 데이터 파이프라인 설계 및 구현

2. **데이터 분석**
   - Pandas를 활용한 데이터 전처리
   - 탐색적 데이터 분석 (EDA)
   - 지리 데이터 시각화

3. **소프트웨어 엔지니어링**
   - 모듈화된 코드 구조
   - 로깅 및 에러 핸들링
   - 환경 관리 및 배포 준비

## 📝 개발 로드맵

- [x] Phase 1: MVP 완성 (데이터 수집 → 저장 → 시각화)
- [ ] Phase 2: 파이프라인 자동화 (Airflow)
- [ ] Phase 3: 스케일업 (Spark, PostGIS)
- [ ] Phase 4: ML 고도화 (예측 모델, 클러스터링)

## 📄 라이선스

MIT License

## 👤 개발자

포트폴리오 프로젝트 | 2025
