# config/logging.py
from loguru import logger
import sys
from pathlib import Path


def setup_logger():
    """프로젝트 전역 로거 설정"""

    # 기본 로거 제거 (중복 방지)
    logger.remove()

    # 1. 콘솔 출력 (INFO 레벨 이상)
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO",
        colorize=True,
    )

    # 2. 파일 출력 - 일반 로그 (DEBUG 레벨 이상, 날짜별 로테이션)
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    logger.add(
        log_dir / "app_{time:YYYY-MM-DD}.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG",
        rotation="00:00",  # 매일 자정에 새 파일
        retention="30 days",  # 30일치 보관
        compression="zip",  # 오래된 로그 압축
        encoding="utf-8",
    )

    # 3. 파일 출력 - 에러 로그만 (ERROR 레벨 이상)
    logger.add(
        log_dir / "error_{time:YYYY-MM-DD}.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="ERROR",
        rotation="00:00",
        retention="90 days",  # 에러는 90일 보관
        compression="zip",
        encoding="utf-8",
    )

    logger.info("로거 초기화 완료")
    return logger
