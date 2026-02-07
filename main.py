# main.py
import json
import asyncio
import pandas as pd
from src.collector import Collector
from src.storage import DataStorage
from src.preprocessor import DataPreprocessor
from config.logging import logger


def print_json(json_data: dict | list) -> None:
    """JSON 데이터를 가독성 좋게 출력하는 유틸리티 함수

    Args:
        json_data: 출력할 JSON 데이터 (딕셔너리 또는 리스트)
    """
    print(json.dumps(json_data, indent=2, ensure_ascii=False))
    return


async def collect_and_save(
    sido: str, sigungu: str, force_update: bool = False
) -> pd.DataFrame:
    """상가업소 데이터 수집 및 저장

    Args:
        sido: 시도명 (예: "서울특별시")
        sigungu: 시군구명 (예: "강남구")
        force_update: True면 강제로 API 호출, False면 기존 파일 우선 사용
    """
    storage = DataStorage()

    # 1. 기존 파일 확인
    if not force_update and storage.file_exists(sido, sigungu):
        logger.info(f"기존 데이터 파일 사용: {sido} {sigungu}")
        df = storage.load_stores(sido, sigungu)
        return df

    # 2. 파일이 없거나 force_update=True인 경우 API 호출
    logger.info(f"API 호출하여 데이터 수집 시작: {sido} {sigungu}")
    async with Collector() as collector:
        df = await collector.collect_stores(sido, sigungu)

    # 3. 수집한 데이터 저장
    if not df.empty:
        storage.save_stores(df, sido, sigungu, format="parquet")
    else:
        logger.warning(f"수집된 데이터가 없습니다: {sido} {sigungu}")

    return df


async def test_preprocessing() -> None:
    """전처리 테스트 함수"""
    try:
        logger.info("=== 전처리 테스트 시작 ===")

        # 1. Raw 데이터 로드
        storage = DataStorage()
        preprocessor = DataPreprocessor()

        sido = "서울특별시"
        sigungu = "강남구"

        # Raw 데이터 로드
        df_raw = storage.load_stores(sido, sigungu)
        if df_raw is None:
            logger.error("Raw 데이터가 없습니다. 먼저 데이터를 수집하세요.")
            return

        print(f"\n=== Raw 데이터 정보 ===")
        print(f"형태: {df_raw.shape}")
        print(f"컬럼 수: {len(df_raw.columns)}")
        print(f"\n결측치 현황 (상위 10개):")
        missing = df_raw.isna().sum().sort_values(ascending=False).head(10)
        print(missing)

        # 2. 전처리 실행
        df_processed = preprocessor.preprocess(df_raw)

        # 3. 전처리 결과 출력
        print(f"\n=== 전처리 후 데이터 정보 ===")
        print(f"형태: {df_processed.shape}")
        print(f"제거된 행: {len(df_raw) - len(df_processed)} 건")

        # 4. 요약 정보
        summary = preprocessor.get_summary(df_processed)
        print(f"\n=== 요약 정보 ===")
        for key, value in summary.items():
            print(f"{key}: {value:,}")

        # 5. 숫자형 컬럼 기초 통계
        print(f"\n=== 좌표 기초 통계 ===")
        print(df_processed[["lon", "lat"]].describe())

        # 6. 업종별 분포 (상위 10개)
        print(f"\n=== 업종 대분류 분포 (상위 10) ===")
        print(df_processed["indsLclsNm"].value_counts().head(10))

        print(f"\n=== 업종 중분류 분포 (상위 10) ===")
        print(df_processed["indsMclsNm"].value_counts().head(10))

        # 7. 전처리 데이터 저장
        save_path = preprocessor.save_processed(df_processed, sido, sigungu)
        print(f"\n저장 경로: {save_path}")

        # 8. 샘플 데이터 출력
        print(f"\n=== 전처리 데이터 샘플 (5건) ===")
        sample_cols = [
            "bizesNm",
            "indsLclsNm",
            "indsMclsNm",
            "indsSclsNm",
            "lon",
            "lat",
            "rdnmAdr",
        ]
        print(df_processed[sample_cols].head().to_string(index=False))

    except Exception as e:
        logger.exception("전처리 테스트 중 오류 발생")

    finally:
        logger.info("=== 전처리 테스트 종료 ===")
    return


if __name__ == "__main__":
    asyncio.run(test_preprocessing())
