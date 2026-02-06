# main.py
import json
import asyncio
from src.collector import Collector
from config.logging import logger


def print_json(json_data: dict | list) -> None:
    """JSON 데이터를 가독성 좋게 출력하는 유틸리티 함수

    Args:
        json_data: 출력할 JSON 데이터 (딕셔너리 또는 리스트)
    """
    print(json.dumps(json_data, indent=2, ensure_ascii=False))
    return


async def main() -> None:
    """테스트용 메인 함수"""
    try:
        logger.info("=== 프로그램 시작 ===")
        async with Collector() as collector:
            sido, sigungu = "서울특별시", "강동구"
            df = await collector.collect_stores(sido, sigungu)
            logger.info(f"데이터 형태: {df.shape}")
        print(df)
        return

    except Exception as e:
        logger.exception(f"프로그램 실행 중 오류 발생")

    finally:
        logger.info("=== 프로그램 종료 ===")

    return


if __name__ == "__main__":
    asyncio.run(main())
