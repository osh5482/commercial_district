# src/database.py
import json
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from sqlalchemy import (
    create_engine,
    text,
    inspect,
    Integer,
    String,
    Float,
    Boolean,
    Text,
    MetaData,
    Table,
    Column,
    Index,
)
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from geoalchemy2 import Geometry
from config.logging import logger
from config.settings import POSTGRES_URL


class DatabaseManager:
    """PostgreSQL + PostGIS 데이터베이스 관리 클래스

    전처리된 상가업소 데이터를 PostgreSQL DB에 저장하고 조회하는 기능 제공
    - 메타데이터 기반 자동 스키마 생성
    - 영문/한글 컬럼명 매핑
    - 배치 삽입 및 인덱스 최적화
    - PostGIS 지리 데이터 타입 지원
    - 연결 풀링을 통한 성능 최적화
    """

    def __init__(self, db_url: str = None):
        """DatabaseManager 초기화

        Args:
            db_url: PostgreSQL 연결 URL (기본값: config.settings.POSTGRES_URL)
                   형식: postgresql://user:password@host:port/database
        """
        self.db_url = db_url or POSTGRES_URL
        self.engine = None
        self.conn = None
        self.column_mapping: Dict[str, Dict[str, str]] = {}

        logger.info(f"DatabaseManager 초기화: {self._safe_url()}")

    def _safe_url(self) -> str:
        """비밀번호를 마스킹한 안전한 연결 URL 반환"""
        # postgresql://user:***@host:port/database 형태로 변환
        if not self.db_url:
            return "None"
        parts = self.db_url.split("@")
        if len(parts) == 2:
            user_part = parts[0].split(":")[0]
            return f"{user_part}:***@{parts[1]}"
        return "***"

    def connect(self):
        """PostgreSQL 데이터베이스 연결 (연결 풀링 사용)

        Returns:
            SQLAlchemy Connection 객체
        """
        if self.engine is None:
            # SQLAlchemy Engine 생성 (연결 풀링 활성화)
            self.engine = create_engine(
                self.db_url,
                poolclass=QueuePool,
                pool_size=5,  # 기본 연결 풀 크기
                max_overflow=10,  # 추가 연결 가능 수
                pool_pre_ping=True,  # 연결 유효성 사전 검사
                echo=False,  # SQL 로깅 비활성화 (필요시 True)
            )
            logger.info(f"DB Engine 생성 완료: {self._safe_url()}")

        if self.conn is None:
            self.conn = self.engine.connect()
            logger.info("DB 연결 성공")

        return self.conn

    def close(self) -> None:
        """데이터베이스 연결 종료"""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("DB 연결 종료")

    def dispose(self) -> None:
        """엔진 및 연결 풀 완전 종료"""
        if self.engine:
            self.engine.dispose()
            self.engine = None
            self.conn = None
            logger.info("DB 엔진 및 연결 풀 종료")

    def __enter__(self):
        """컨텍스트 매니저 진입"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """컨텍스트 매니저 종료"""
        self.close()

    def _load_metadata(self, metadata_path: str = "config/columns.json") -> Dict:
        """메타데이터 파일 로드 및 컬럼 매핑 생성

        Args:
            metadata_path: 컬럼 메타데이터 파일 경로

        Returns:
            메타데이터 딕셔너리

        Raises:
            FileNotFoundError: 메타데이터 파일이 없는 경우
            json.JSONDecodeError: JSON 파싱 실패
        """
        metadata_file = Path(metadata_path)

        if not metadata_file.exists():
            logger.error(f"메타데이터 파일 없음: {metadata_file}")
            raise FileNotFoundError(
                f"메타데이터 파일이 존재하지 않습니다: {metadata_file}"
            )

        try:
            with open(metadata_file, "r", encoding="utf-8") as f:
                metadata = json.load(f)

            # 컬럼 매핑 딕셔너리 생성
            raw_to_english = {}
            korean_to_english = {}
            english_to_korean = {}

            for col in metadata["columns"]:
                raw_name = col["raw"]
                english_name = col["english"]
                korean_name = col["korean"]

                raw_to_english[raw_name] = english_name
                korean_to_english[korean_name] = english_name
                english_to_korean[english_name] = korean_name

            self.column_mapping = {
                "columns": metadata["columns"],
                "raw_to_english": raw_to_english,
                "korean_to_english": korean_to_english,
                "english_to_korean": english_to_korean,
            }

            logger.info(f"메타데이터 로드 완료: {len(metadata['columns'])} 개 컬럼")
            return metadata

        except json.JSONDecodeError as e:
            logger.error(f"메타데이터 파싱 실패: {e}")
            raise

    def _map_type_to_sqlalchemy(self, col_type: str, col_name: str):
        """메타데이터 타입을 SQLAlchemy 타입으로 변환

        Args:
            col_type: 메타데이터의 타입 문자열 (예: "TEXT", "INTEGER")
            col_name: 컬럼명 (geometry 판단용)

        Returns:
            SQLAlchemy 타입 객체
        """
        # 위도/경도 컬럼은 PostGIS Geometry 타입으로 처리 (나중에 확장 가능)
        # 현재는 DOUBLE로 유지 (기존 데이터 호환성)
        type_mapping = {
            "TEXT": Text,
            "INTEGER": Integer,
            "REAL": Float,
            "DOUBLE": Float,
            "BOOLEAN": Boolean,
        }

        # 기본 타입 매핑
        sqlalchemy_type = type_mapping.get(col_type.upper(), Text)

        # VARCHAR 처리 (예: "VARCHAR(255)")
        if "VARCHAR" in col_type.upper():
            # VARCHAR 길이 추출 (예: VARCHAR(255) -> 255)
            try:
                length = int(col_type.split("(")[1].split(")")[0])
                return String(length)
            except:
                return String(255)  # 기본 길이

        return sqlalchemy_type

    def create_table_from_metadata(
        self,
        table_name: str = "stores",
        metadata_path: str = "config/columns.json",
        df: Optional[pd.DataFrame] = None,
    ) -> None:
        """메타데이터 기반 테이블 생성 (DataFrame 컬럼에 맞춰 동적 생성)

        Args:
            table_name: 생성할 테이블명 (기본값: "stores")
            metadata_path: 컬럼 메타데이터 파일 경로
            df: 참조할 DataFrame (제공되면 해당 컬럼만 테이블에 포함)

        Raises:
            SQLAlchemyError: 테이블 생성 실패
        """
        # 메타데이터 로드
        metadata_json = self._load_metadata(metadata_path)

        # Header 컬럼 (API 메타정보) 제외
        header_columns = {
            "description",
            "columns",
            "stdr_ym",
            "result_code",
            "result_msg",
        }

        # DataFrame이 제공된 경우 실제 존재하는 컬럼만 필터링
        if df is not None:
            # DataFrame의 raw 컬럼명을 english 컬럼명으로 변환
            raw_to_english = self.column_mapping["raw_to_english"]
            existing_columns = set()

            for raw_col in df.columns:
                if raw_col in raw_to_english:
                    english_col = raw_to_english[raw_col]
                    if english_col not in header_columns:
                        existing_columns.add(english_col)

            logger.info(f"DataFrame 기반 테이블 생성: {len(existing_columns)} 컬럼")
        else:
            existing_columns = None

        # SQLAlchemy MetaData 및 Column 정의 생성
        metadata_obj = MetaData()
        columns = []

        for col in metadata_json["columns"]:
            english_name = col["english"]
            col_type = col["type"]

            # Header 컬럼 제외
            if english_name in header_columns:
                continue

            # DataFrame이 제공된 경우 해당 컬럼만 포함
            if existing_columns is not None and english_name not in existing_columns:
                continue

            # SQLAlchemy 타입 변환
            sqlalchemy_type = self._map_type_to_sqlalchemy(col_type, english_name)

            # PRIMARY KEY 설정 (상가업소번호)
            if english_name == "bizes_id":
                columns.append(Column(english_name, sqlalchemy_type, primary_key=True))
            # NOT NULL 설정 (필수 컬럼)
            elif english_name in [
                "bizes_nm",
                "inds_lcls_nm",
                "inds_mcls_nm",
                "inds_scls_nm",
            ]:
                columns.append(Column(english_name, sqlalchemy_type, nullable=False))
            else:
                columns.append(Column(english_name, sqlalchemy_type))

        # Table 객체 생성
        table = Table(table_name, metadata_obj, *columns)

        try:
            # 테이블 생성 (이미 존재하면 스킵)
            metadata_obj.create_all(self.engine, checkfirst=True)
            logger.success(f"테이블 생성 완료: {table_name} ({len(columns)} 컬럼)")
            logger.debug(f"생성된 컬럼: {[col.name for col in columns]}")

        except SQLAlchemyError as e:
            logger.error(f"테이블 생성 실패: {e}")
            raise

    def create_indexes(self, table_name: str = "stores") -> None:
        """성능 최적화를 위한 인덱스 생성 (테이블에 존재하는 컬럼만)

        Args:
            table_name: 인덱스를 생성할 테이블명

        Raises:
            SQLAlchemyError: 인덱스 생성 실패
        """
        # Inspector로 테이블의 실제 컬럼 조회
        inspector = inspect(self.engine)
        existing_columns = {col["name"] for col in inspector.get_columns(table_name)}

        logger.debug(f"테이블 '{table_name}'의 컬럼: {existing_columns}")

        # 인덱스 정의 (이름, 컬럼 리스트)
        index_configs = [
            ("idx_region", ["ctprvn_nm", "signgu_nm", "adong_nm"]),
            ("idx_industry", ["inds_lcls_nm", "inds_mcls_nm", "inds_scls_nm"]),
            ("idx_lon", ["lon"]),
            ("idx_lat", ["lat"]),
            ("idx_trar", ["trar_no"]),
            ("idx_signgu_cd", ["signgu_cd"]),
        ]

        created_indexes = []

        try:
            for idx_name, idx_columns in index_configs:
                # 모든 컬럼이 테이블에 존재하는지 확인
                if all(col in existing_columns for col in idx_columns):
                    # CREATE INDEX IF NOT EXISTS 실행
                    idx_sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name}({', '.join(idx_columns)})"
                    self.conn.execute(text(idx_sql))
                    self.conn.commit()
                    logger.debug(f"인덱스 생성: {idx_name} ({', '.join(idx_columns)})")
                    created_indexes.append(idx_name)
                else:
                    missing_cols = [
                        col for col in idx_columns if col not in existing_columns
                    ]
                    logger.warning(
                        f"인덱스 '{idx_name}' 생략: 컬럼 없음 ({', '.join(missing_cols)})"
                    )

            logger.success(
                f"인덱스 생성 완료: {len(created_indexes)} 개 (생략: {len(index_configs) - len(created_indexes)} 개)"
            )

        except SQLAlchemyError as e:
            logger.error(f"인덱스 생성 실패: {e}")
            raise

    def insert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str = "stores",
        if_exists: str = "append",
        batch_size: int = 10000,
        recreate_table: bool = False,
    ) -> int:
        """DataFrame 데이터를 DB에 삽입

        Args:
            df: 삽입할 DataFrame (raw 컬럼명 사용)
            table_name: 대상 테이블명
            if_exists: 중복 처리 방법 ("append", "replace", "fail")
            batch_size: 배치 삽입 크기 (메모리 최적화)
            recreate_table: True면 테이블 재생성 (제약조건, 인덱스 유지)

        Returns:
            삽입된 레코드 수

        Raises:
            ValueError: 컬럼 매핑 실패
            SQLAlchemyError: DB 삽입 실패
        """
        if not self.column_mapping:
            logger.warning("컬럼 매핑이 로드되지 않음. 메타데이터 로드 시도...")
            self._load_metadata()

        # DataFrame 복사 (원본 보존)
        df_copy = df.copy()

        # 컬럼명 변환: raw → english (snake_case)
        rename_map = self.column_mapping["raw_to_english"]

        # 존재하는 컬럼만 변환
        columns_to_rename = {
            k: v for k, v in rename_map.items() if k in df_copy.columns
        }
        df_copy.rename(columns=columns_to_rename, inplace=True)

        # Header 컬럼 제거 (DB 스키마에 없음)
        header_columns = {
            "description",
            "columns",
            "stdr_ym",
            "result_code",
            "result_msg",
            "total_count",
            "num_of_rows",
            "page_no",
        }
        columns_to_drop = [col for col in df_copy.columns if col in header_columns]
        if columns_to_drop:
            df_copy.drop(columns=columns_to_drop, inplace=True)
            logger.debug(f"Header 컬럼 제거: {columns_to_drop}")

        # recreate_table=True인 경우만 테이블 재생성
        if recreate_table:
            logger.info(f"테이블 재생성 중: {table_name}")

            # 1. 기존 테이블 삭제
            drop_sql = f"DROP TABLE IF EXISTS {table_name} CASCADE"
            self.conn.execute(text(drop_sql))
            self.conn.commit()
            logger.debug(f"기존 테이블 삭제: {table_name}")

            # 2. 제약조건이 있는 테이블 재생성 (원본 DataFrame 사용)
            self.create_table_from_metadata(table_name=table_name, df=df)

            # 3. 인덱스 생성
            self.create_indexes(table_name=table_name)

            # 4. 데이터는 append 모드로 삽입
            actual_if_exists = "append"
        else:
            actual_if_exists = if_exists

        logger.info(f"데이터 삽입 시작: {len(df_copy)} 건 ({actual_if_exists} 모드)")

        try:
            # DataFrame → PostgreSQL 삽입
            df_copy.to_sql(
                name=table_name,
                con=self.engine,  # engine 사용 (connection 아님)
                if_exists=actual_if_exists,
                index=False,
                chunksize=batch_size,
                method="multi",  # 배치 삽입 최적화
            )

            logger.success(f"데이터 삽입 완료: {len(df_copy)} 건")
            return len(df_copy)

        except IntegrityError as e:
            logger.error(f"중복 키 오류: {e}")
            raise
        except SQLAlchemyError as e:
            logger.error(f"데이터 삽입 실패: {e}")
            raise

    def query(self, sql: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """SQL 쿼리 실행 및 결과 반환

        Args:
            sql: 실행할 SQL 쿼리
            params: 쿼리 파라미터 (:name placeholder 사용)

        Returns:
            쿼리 결과 DataFrame

        Raises:
            SQLAlchemyError: 쿼리 실행 실패
        """
        try:
            logger.debug(f"SQL 실행: {sql}")
            if params:
                logger.debug(f"파라미터: {params}")

            # SQLAlchemy text() 사용
            df = pd.read_sql_query(text(sql), self.conn, params=params)
            logger.info(f"쿼리 완료: {len(df)} 건")
            return df

        except SQLAlchemyError as e:
            logger.error(f"쿼리 실행 오류: {e}")
            logger.error(f"SQL: {sql}")
            raise

    def query_korean(
        self,
        korean_columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, str]] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """한글 컬럼명으로 데이터 조회

        Args:
            korean_columns: 조회할 한글 컬럼명 리스트 (None이면 전체)
            filters: 필터 조건 {"한글컬럼명": "값"}
            limit: 결과 제한 수

        Returns:
            한글 컬럼명의 DataFrame

        Raises:
            KeyError: 존재하지 않는 한글 컬럼명
            SQLAlchemyError: 쿼리 실행 실패
        """
        if not self.column_mapping:
            logger.warning("컬럼 매핑이 로드되지 않음. 메타데이터 로드 시도...")
            self._load_metadata()

        korean_to_english = self.column_mapping["korean_to_english"]
        english_to_korean = self.column_mapping["english_to_korean"]

        # 1. SELECT 절 생성
        if korean_columns:
            # 한글 → 영문 변환
            try:
                english_cols = [korean_to_english[k] for k in korean_columns]
                select_clause = ", ".join(english_cols)
            except KeyError as e:
                available_cols = list(korean_to_english.keys())
                logger.error(f"존재하지 않는 컬럼: {e}")
                logger.error(f"사용 가능한 컬럼: {available_cols[:10]}...")
                raise KeyError(f"존재하지 않는 한글 컬럼명: {e}")
        else:
            select_clause = "*"
            english_cols = list(english_to_korean.keys())

        # 2. WHERE 절 생성
        where_clause = ""
        params = {}

        if filters:
            try:
                conditions = []
                for i, (korean_col, value) in enumerate(filters.items()):
                    english_col = korean_to_english[korean_col]
                    param_name = f"param_{i}"
                    conditions.append(f"{english_col} = :{param_name}")
                    params[param_name] = value

                where_clause = " WHERE " + " AND ".join(conditions)
            except KeyError as e:
                available_cols = list(korean_to_english.keys())
                logger.error(f"필터 컬럼 오류: {e}")
                logger.error(f"사용 가능한 컬럼: {available_cols[:10]}...")
                raise KeyError(f"존재하지 않는 한글 컬럼명: {e}")

        # 3. LIMIT 절 생성
        limit_clause = f" LIMIT {limit}" if limit else ""

        # 4. 최종 SQL 생성
        sql = f"SELECT {select_clause} FROM stores{where_clause}{limit_clause}"

        # 5. 쿼리 실행
        df = self.query(sql, params if params else None)

        # 6. 컬럼명 영문 → 한글 변환
        if korean_columns:
            # 명시적으로 지정한 컬럼만 변환
            rename_map = {
                english_to_korean[e]: e for e in english_cols if e in english_to_korean
            }
            df.rename(columns={v: k for k, v in rename_map.items()}, inplace=True)
        else:
            # 전체 컬럼 변환
            rename_map = {e: k for e, k in english_to_korean.items() if e in df.columns}
            df.rename(columns=rename_map, inplace=True)

        return df

    def get_column_mapping(self) -> Dict[str, Dict[str, str]]:
        """컬럼 매핑 정보 반환

        Returns:
            컬럼 매핑 딕셔너리
        """
        if not self.column_mapping:
            self._load_metadata()
        return self.column_mapping

    def table_exists(self, table_name: str = "stores") -> bool:
        """테이블 존재 여부 확인

        Args:
            table_name: 확인할 테이블명

        Returns:
            테이블 존재 여부
        """
        try:
            inspector = inspect(self.engine)
            exists = table_name in inspector.get_table_names()

            if exists:
                logger.debug(f"테이블 '{table_name}' 존재 확인")
            else:
                logger.debug(f"테이블 '{table_name}' 존재하지 않음")

            return exists

        except SQLAlchemyError as e:
            logger.error(f"테이블 존재 확인 실패: {e}")
            raise

    def get_region_data_count(
        self, sido: str, sigungu: str, table_name: str = "stores"
    ) -> int:
        """특정 지역의 데이터 건수 조회

        Args:
            sido: 시도명 (예: "서울특별시")
            sigungu: 시군구명 (예: "강남구")
            table_name: 조회할 테이블명

        Returns:
            해당 지역의 데이터 건수
        """
        try:
            # 테이블이 없으면 0 반환
            if not self.table_exists(table_name):
                return 0

            sql = f"""
                SELECT COUNT(*) as count
                FROM {table_name}
                WHERE ctprvn_nm = :sido AND signgu_nm = :sigungu
            """
            result = self.conn.execute(
                text(sql), {"sido": sido, "sigungu": sigungu}
            ).fetchone()
            count = result[0] if result else 0
            logger.debug(f"{sido} {sigungu} 데이터: {count} 건")
            return count

        except SQLAlchemyError as e:
            logger.error(f"지역 데이터 건수 조회 실패: {e}")
            raise

    def delete_region_data(
        self, sido: str, sigungu: str, table_name: str = "stores"
    ) -> int:
        """특정 지역의 데이터 삭제

        Args:
            sido: 시도명
            sigungu: 시군구명
            table_name: 대상 테이블명

        Returns:
            삭제된 레코드 수
        """
        try:
            sql = f"""
                DELETE FROM {table_name}
                WHERE ctprvn_nm = :sido AND signgu_nm = :sigungu
            """
            result = self.conn.execute(text(sql), {"sido": sido, "sigungu": sigungu})
            self.conn.commit()
            deleted_count = result.rowcount
            logger.info(f"{sido} {sigungu} 데이터 삭제: {deleted_count} 건")
            return deleted_count

        except SQLAlchemyError as e:
            logger.error(f"지역 데이터 삭제 실패: {e}")
            raise

    def get_stats(self, table_name: str = "stores") -> Dict[str, any]:
        """테이블 통계 정보 조회

        Args:
            table_name: 조회할 테이블명

        Returns:
            통계 정보 딕셔너리
        """
        try:
            # 총 레코드 수
            sql_count = f"SELECT COUNT(*) as count FROM {table_name}"
            total_count = self.conn.execute(text(sql_count)).fetchone()[0]

            # 테이블 크기 (MB) - PostgreSQL pg_total_relation_size 사용
            sql_size = f"""
                SELECT pg_total_relation_size('{table_name}') / (1024.0 * 1024.0) as size_mb
            """
            table_size_mb = self.conn.execute(text(sql_size)).fetchone()[0]

            # 시도 수
            sql_sido = f"SELECT COUNT(DISTINCT ctprvn_nm) as count FROM {table_name}"
            sido_count = self.conn.execute(text(sql_sido)).fetchone()[0]

            # 시군구 수
            sql_sigungu = f"SELECT COUNT(DISTINCT signgu_nm) as count FROM {table_name}"
            sigungu_count = self.conn.execute(text(sql_sigungu)).fetchone()[0]

            # 업종 대분류 수
            sql_industry = (
                f"SELECT COUNT(DISTINCT inds_lcls_nm) as count FROM {table_name}"
            )
            industry_count = self.conn.execute(text(sql_industry)).fetchone()[0]

            stats = {
                "총_레코드_수": total_count,
                "테이블_크기_MB": round(table_size_mb, 2),
                "시도_수": sido_count,
                "시군구_수": sigungu_count,
                "업종대분류_수": industry_count,
            }

            logger.info(f"테이블 통계: {stats}")
            return stats

        except SQLAlchemyError as e:
            logger.error(f"통계 조회 실패: {e}")
            raise
