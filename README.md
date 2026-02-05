# Commercial District Analysis

전국 상권 데이터 수집 및 분석 파이프라인

## 프로젝트 목표

- 공공데이터 기반 상권 분석 플랫폼 구축
- 데이터 수집 → 전처리 → 시각화 전 과정 경험

## 기술 스택

- Python 3.12
- Pandas, Streamlit
- (예정) Airflow, Spark, PostgreSQL

## 설치 방법

```bash
conda env create -f environment.yml
conda activate commercial_district
```

## 데이터 출처

- 소상공인시장진흥공단 상가(상권)정보 API (https://www.data.go.kr/data/15012005/openapi.do#/)
