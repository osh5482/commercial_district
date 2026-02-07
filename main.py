# main.py
import json
import asyncio
from src.collector import Collector
from src.storage import DataStorage
from config.logging import logger


def print_json(json_data: dict | list) -> None:
    """JSON 데이터를 가독성 좋게 출력하는 유틸리티 함수

    Args:
        json_data: 출력할 JSON 데이터 (딕셔너리 또는 리스트)
    """
    print(json.dumps(json_data, indent=2, ensure_ascii=False))
    return


async def collect_and_save(sido: str, sigungu: str, force_update: bool = False):
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


async def main() -> None:
    """테스트용 메인 함수"""
    try:
        logger.info("=== 프로그램 시작 ===")

        # 데이터 수집 설정
        sido = "서울특별시"
        sigungu = "강남구"
        force_update = False  # True로 변경하면 강제로 API 호출

        df = await collect_and_save(sido, sigungu, force_update)

        # 기본 정보 출력
        logger.info(f"데이터 형태: {df.shape}")
        logger.info(f"컬럼: {df.columns.tolist()}")

        print("\n=== 데이터 미리보기 ===")
        print(df.head())

        print("\n=== 기본 통계 ===")
        print(df.describe())

        # 저장된 파일 정보 확인
        storage = DataStorage()
        file_info = storage.get_file_info()
        if not file_info.empty:
            print("\n=== 저장된 파일 목록 ===")
            print(file_info.to_string(index=False))

    except Exception as e:
        logger.exception(f"프로그램 실행 중 오류 발생")

    finally:
        logger.info("=== 프로그램 종료 ===")

    return


if __name__ == "__main__":
    asyncio.run(main())
