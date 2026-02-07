# main.py
import json
import asyncio
import pandas as pd
from src.collector import Collector
from src.storage import DataStorage
from src.preprocessor import DataPreprocessor
from src.database import DatabaseManager
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


def save_to_database(sido: str, sigungu: str) -> None:
    """전처리된 데이터를 DB에 저장

    Args:
        sido: 시도명 (예: "서울특별시")
        sigungu: 시군구명 (예: "강남구")
    """
    try:
        logger.info(f"=== DB 저장 시작: {sido} {sigungu} ===")

        # 1. 전처리 데이터 로드
        preprocessor = DataPreprocessor()
        df = preprocessor.load_processed(sido, sigungu)

        if df is None or df.empty:
            logger.error(f"전처리 데이터가 없습니다: {sido} {sigungu}")
            logger.error("실행 방법: test_preprocessing() 함수 실행 후 다시 시도")
            return

        print(f"\n=== 로드된 데이터 정보 ===")
        print(f"지역: {sido} {sigungu}")
        print(f"총 레코드 수: {len(df):,} 건")
        print(f"컬럼 수: {len(df.columns)} 개")

        # 2. DB 연결 및 데이터 삽입
        with DatabaseManager() as db:
            # 2-1. 테이블 존재 여부 확인
            table_exists = db.table_exists("stores")

            if table_exists:
                logger.info("테이블 'stores'가 이미 존재합니다.")

                # 2-2. 해당 지역 데이터 존재 여부 확인
                existing_count = db.get_region_data_count(sido, sigungu)

                if existing_count > 0:
                    logger.warning(
                        f"{sido} {sigungu} 데이터가 이미 {existing_count:,}건 존재합니다."
                    )
                    print(f"\n=== 중복 데이터 발견 ===")
                    print(f"기존 데이터: {existing_count:,} 건")
                    print(f"새 데이터: {len(df):,} 건")
                    print(f"동작: 기존 데이터를 삭제하고 새 데이터로 교체합니다.")

                    # 기존 데이터 삭제
                    deleted_count = db.delete_region_data(sido, sigungu)
                    logger.info(f"기존 데이터 삭제 완료: {deleted_count:,} 건")

                    # 새 데이터 삽입 (append 모드)
                    inserted_count = db.insert_dataframe(df, if_exists="append")
                    print(f"\n=== 데이터 교체 완료 ===")
                    print(f"삭제: {deleted_count:,} 건")
                    print(f"삽입: {inserted_count:,} 건")
                else:
                    # 해당 지역 데이터가 없으면 새로 삽입
                    logger.info(f"{sido} {sigungu} 데이터가 없습니다. 새로 삽입합니다.")
                    inserted_count = db.insert_dataframe(df, if_exists="append")
                    print(f"\n=== 데이터 삽입 완료 ===")
                    print(f"삽입된 레코드 수: {inserted_count:,} 건")
            else:
                # 테이블이 없으면 생성 후 삽입
                logger.info("테이블이 없습니다. 새로 생성합니다.")

                # 테이블 생성
                db.create_table_from_metadata(df=df)
                db.create_indexes()

                # 데이터 삽입
                inserted_count = db.insert_dataframe(df, if_exists="append")
                print(f"\n=== 테이블 생성 및 데이터 삽입 완료 ===")
                print(f"삽입된 레코드 수: {inserted_count:,} 건")

            # 3. 최종 통계 조회
            stats = db.get_stats()
            print(f"\n=== 데이터베이스 통계 (전체) ===")
            for key, value in stats.items():
                print(f"{key}: {value:,}")

        logger.success(f"=== DB 저장 완료: {sido} {sigungu} ===")

    except Exception as e:
        logger.exception(f"DB 저장 중 오류 발생: {sido} {sigungu}")


def query_database(sigungu: str = None, keyword: str = None) -> None:
    """DB에서 데이터 조회 및 다양한 분석 테스트

    Args:
        sigungu: 조회할 시군구명 (None이면 전체 조회)
        keyword: 상호명에서 검색할 키워드 (예: '스타벅스', '편의점')
    """
    try:
        logger.info("=== DB 조회 및 분석 테스트 시작 ===")

        with DatabaseManager() as db:
            # 1. 테이블 존재 확인
            if not db.table_exists("stores"):
                logger.error("테이블이 존재하지 않습니다. 먼저 데이터를 저장하세요.")
                return

            # 2. 전체 및 지역별 통계
            stats = db.get_stats()
            print(f"\n=== [1] 데이터베이스 기본 통계 ===")
            for key, value in stats.items():
                print(f"  * {key}: {value:,}")

            if sigungu:
                count = db.get_region_data_count("서울특별시", sigungu)
                print(f"  * {sigungu} 데이터 건수: {count:,} 건")

            # 3. 업종 계층별 통계 (대/중/소분류)
            print(f"\n=== [2] 업종 계층별 Top 5 분포 ===")
            levels = [
                ("inds_lcls_nm", "대분류"),
                ("inds_mcls_nm", "중분류"),
                ("inds_scls_nm", "소분류"),
            ]

            for col, label in levels:
                where_clause = f"WHERE signgu_nm = '{sigungu}'" if sigungu else ""
                sql = f"SELECT {col} as name, COUNT(*) as count FROM stores {where_clause} GROUP BY {col} ORDER BY count DESC LIMIT 5"
                df_top = db.query(sql)
                print(f"\n  < {label} 상위 5 >")
                for _, row in df_top.iterrows():
                    print(f"    - {row['name']}: {row['count']:,} 건")

            # 4. 지역별 상세 분포 (행정동별)
            print(f"\n=== [3] 행정동별 상가 분포 (Top 10) ===")
            where_clause = f"WHERE signgu_nm = '{sigungu}'" if sigungu else ""
            sql = f"SELECT adong_nm, COUNT(*) as count FROM stores {where_clause} GROUP BY adong_nm ORDER BY count DESC LIMIT 10"
            df_dong = db.query(sql)
            for i, row in df_dong.iterrows():
                print(f"  {i+1}. {row['adong_nm']}: {row['count']:,} 건")

            # 5. 키워드 검색 테스트 (키워드가 지정된 경우만)
            print(f"\n=== [4] 키워드 검색 테스트 ===", end="")
            if keyword:
                print(f" (키워드: '{keyword}') ===")
                # LIKE 연산자를 사용하여 부분 일치 검색
                search_sql = f"SELECT bizes_nm, inds_scls_nm, rdnm_adr FROM stores WHERE bizes_nm LIKE ? LIMIT 10"
                df_search = db.query(search_sql, (f"%{keyword}%",))

                if not df_search.empty:
                    print(df_search.to_string(index=False))
                else:
                    print(f"  '{keyword}' 검색 결과가 없습니다.")
            else:
                print("\n키워드가 지정되지 않았습니다. 키워드 검색을 패스합니다.")

            # 6. 데이터 품질 체크 (좌표 누락 등)
            print(f"\n=== [5] 데이터 품질 체크 ===")
            quality_sql = """
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN lon IS NULL OR lat IS NULL THEN 1 ELSE 0 END) as missing_coords,
                    SUM(CASE WHEN bizes_nm IS NULL OR bizes_nm = '' THEN 1 ELSE 0 END) as missing_names
                FROM stores
            """
            df_quality = db.query(quality_sql)
            row = df_quality.iloc[0]
            print(f"  * 전체 데이터: {row['total']:,} 건")
            print(f"  * 좌표 누락: {row['missing_coords']:,} 건")
            print(f"  * 상호명 누락: {row['missing_names']:,} 건")

            # 7. 좌표 범위 검색 (강남역 인근 또는 데이터가 있는 곳)
            print(f"\n=== [6] 공간 쿼리 테스트 (강남역 인근) ===")
            # 강남역 중심 좌표: 127.0276, 37.4979
            geo_sql = """
                SELECT bizes_nm, inds_mcls_nm, lon, lat
                FROM stores
                WHERE lon BETWEEN 127.025 AND 127.030
                  AND lat BETWEEN 37.495 AND 37.500
                LIMIT 5
            """
            df_geo = db.query(geo_sql)
            if not df_geo.empty:
                print(df_geo.to_string(index=False))
            else:
                print("  해당 범위에 데이터가 없습니다.")

        logger.success("=== DB 조회 및 분석 테스트 완료 ===")

    except Exception as e:
        logger.exception("DB 조회 중 오류 발생")


if __name__ == "__main__":
    # 사용 예시:

    # 1. 데이터 수집 및 저장 (최초 1회 또는 업데이트 필요시)
    # asyncio.run(collect_and_save("서울특별시", "강남구"))
    # save_to_database("서울특별시", "강남구")

    # 2. 데이터 조회 및 분석 테스트
    # query_database()                       # 전체 조회
    query_database(sigungu="강동구")  # 특정 지역 분석
    # query_database(keyword="커피")  # 키워드 검색 테스트
