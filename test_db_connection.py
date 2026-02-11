#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""PostgreSQL 연결 테스트 스크립트

database.py의 DatabaseManager를 사용하여 PostgreSQL 연결을 테스트합니다.
"""

from src.database import DatabaseManager
from config.logging import logger


def test_connection():
    """PostgreSQL 연결 테스트"""
    logger.info("=" * 60)
    logger.info("PostgreSQL 연결 테스트 시작")
    logger.info("=" * 60)

    try:
        # 1. DatabaseManager 초기화
        logger.info("\n[1단계] DatabaseManager 초기화")
        with DatabaseManager() as db:
            logger.success("✅ DatabaseManager 초기화 성공")

            # 2. 연결 확인
            logger.info("\n[2단계] PostgreSQL 연결 확인")
            logger.success("✅ PostgreSQL 연결 성공")

            # 3. PostGIS Extension 확인
            logger.info("\n[3단계] PostGIS Extension 확인")
            try:
                from sqlalchemy import text
                result = db.conn.execute(text("SELECT PostGIS_Version()"))
                version = result.fetchone()
                logger.success(f"✅ PostGIS 설치 확인: {version[0]}")
            except Exception as e:
                logger.warning(f"⚠️ PostGIS 확인 실패 (선택사항): {e}")
                logger.info("   → PostGIS는 나중에 공간 분석 시 필요합니다")

            # 4. stores 테이블 존재 여부 확인
            logger.info("\n[4단계] stores 테이블 존재 여부 확인")
            exists = db.table_exists("stores")
            if exists:
                logger.success("✅ stores 테이블이 이미 존재합니다")

                # 테이블 통계 조회
                stats = db.get_stats()
                logger.info(f"\n[테이블 통계]")
                for key, value in stats.items():
                    logger.info(f"  - {key}: {value}")
            else:
                logger.warning("⚠️ stores 테이블이 존재하지 않습니다")
                logger.info("  → 데이터 수집 후 테이블이 자동 생성됩니다")

        logger.info("\n" + "=" * 60)
        logger.success("✅ PostgreSQL 연결 테스트 완료!")
        logger.info("=" * 60)
        return True

    except Exception as e:
        logger.error(f"\n❌ 연결 테스트 실패: {e}")
        logger.error(f"에러 타입: {type(e).__name__}")
        logger.error("\n[해결 방법]")
        logger.error("1. PostgreSQL 서비스가 실행 중인지 확인")
        logger.error("2. .env 파일의 PostgreSQL 설정 확인")
        logger.error("   - POSTGRES_HOST: localhost")
        logger.error("   - POSTGRES_PORT: 5432")
        logger.error("   - POSTGRES_DB: commercial_district")
        logger.error("   - POSTGRES_USER: postgres")
        logger.error("   - POSTGRES_PASSWORD: (설치 시 설정한 비밀번호)")
        logger.error("3. commercial_district 데이터베이스가 생성되었는지 확인")
        logger.error("4. PostGIS Extension이 활성화되었는지 확인")
        return False


if __name__ == "__main__":
    test_connection()
