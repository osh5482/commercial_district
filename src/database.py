# src/database.py
import sqlite3
import json
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from config.logging import logger


class DatabaseManager:
    """SQLite 데이터베이스 관리 클래스

    전처리된 상가업소 데이터를 SQLite DB에 저장하고 조회하는 기능 제공
    - 메타데이터 기반 자동 스키마 생성
    - 영문/한글 컬럼명 매핑
    - 배치 삽입 및 인덱스 최적화
    """

    def __init__(self, db_path: str = "data/commercial_district.db"):
        """DatabaseManager 초기화

        Args:
            db_path: SQLite DB 파일 경로 (기본값: "data/commercial_district.db")
        """
        self.db_path = Path(db_path)
        self.conn: Optional[sqlite3.Connection] = None
        self.column_mapping: Dict[str, Dict[str, str]] = {}

        # DB 디렉토리 생성
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"DatabaseManager 초기화: {self.db_path}")

    def connect(self) -> sqlite3.Connection:
        """SQLite 데이터베이스 연결

        Returns:
            sqlite3.Connection 객체
        """
        if self.conn is None:
            self.conn = sqlite3.connect(str(self.db_path))
            logger.info(f"DB 연결 성공: {self.db_path}")
        return self.conn

    def close(self) -> None:
        """데이터베이스 연결 종료"""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("DB 연결 종료")

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
            sqlite3.Error: 테이블 생성 실패
        """
        # 메타데이터 로드
        metadata = self._load_metadata(metadata_path)

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

        # 테이블 컬럼 정의 생성
        column_definitions = []

        for col in metadata["columns"]:
            english_name = col["english"]
            col_type = col["type"]

            # Header 컬럼 제외
            if english_name in header_columns:
                continue

            # DataFrame이 제공된 경우 해당 컬럼만 포함
            if existing_columns is not None and english_name not in existing_columns:
                continue

            # PRIMARY KEY 설정 (상가업소번호)
            if english_name == "bizes_id":
                column_definitions.append(f"{english_name} {col_type} PRIMARY KEY")
            # NOT NULL 설정 (필수 컬럼)
            elif english_name in [
                "bizes_nm",
                "inds_lcls_nm",
                "inds_mcls_nm",
                "inds_scls_nm",
            ]:
                column_definitions.append(f"{english_name} {col_type} NOT NULL")
            else:
                column_definitions.append(f"{english_name} {col_type}")

        # CREATE TABLE SQL 생성
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(column_definitions)}
        )
        """

        try:
            cursor = self.conn.cursor()
            cursor.execute(create_sql)
            self.conn.commit()
            logger.success(
                f"테이블 생성 완료: {table_name} ({len(column_definitions)} 컬럼)"
            )
            logger.debug(f"생성된 컬럼: {[cd.split()[0] for cd in column_definitions]}")

        except sqlite3.Error as e:
            logger.error(f"테이블 생성 실패: {e}")
            raise

    def create_indexes(self, table_name: str = "stores") -> None:
        """성능 최적화를 위한 인덱스 생성 (테이블에 존재하는 컬럼만)

        Args:
            table_name: 인덱스를 생성할 테이블명

        Raises:
            sqlite3.Error: 인덱스 생성 실패
        """
        # 테이블의 실제 컬럼 조회
        cursor = self.conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        existing_columns = {row[1] for row in cursor.fetchall()}

        logger.debug(f"테이블 '{table_name}'의 컬럼: {existing_columns}")

        # 인덱스 정의 (이름, 컬럼 리스트, SQL 템플릿)
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
                    idx_sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name}({', '.join(idx_columns)})"
                    cursor.execute(idx_sql)
                    logger.debug(f"인덱스 생성: {idx_name} ({', '.join(idx_columns)})")
                    created_indexes.append(idx_name)
                else:
                    missing_cols = [
                        col for col in idx_columns if col not in existing_columns
                    ]
                    logger.warning(
                        f"인덱스 '{idx_name}' 생략: 컬럼 없음 ({', '.join(missing_cols)})"
                    )

            self.conn.commit()
            logger.success(
                f"인덱스 생성 완료: {len(created_indexes)} 개 (생략: {len(index_configs) - len(created_indexes)} 개)"
            )

        except sqlite3.Error as e:
            logger.error(f"인덱스 생성 실패: {e}")
            raise

    def insert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str = "stores",
        if_exists: str = "append",
        batch_size: int = 1000,
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
            sqlite3.Error: DB 삽입 실패
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
        # (if_exists='replace'는 pandas의 to_sql()에 맡김)
        if recreate_table:
            logger.info(f"테이블 재생성 중: {table_name}")

            # 1. 기존 테이블 삭제
            cursor = self.conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
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
            # DataFrame → SQLite 삽입
            df_copy.to_sql(
                name=table_name,
                con=self.conn,
                if_exists=actual_if_exists,
                index=False,
                chunksize=batch_size,
            )

            logger.success(f"데이터 삽입 완료: {len(df_copy)} 건")
            return len(df_copy)

        except sqlite3.IntegrityError as e:
            logger.error(f"중복 키 오류: {e}")
            raise
        except sqlite3.Error as e:
            logger.error(f"데이터 삽입 실패: {e}")
            raise

    def query(self, sql: str, params: Optional[Tuple] = None) -> pd.DataFrame:
        """SQL 쿼리 실행 및 결과 반환

        Args:
            sql: 실행할 SQL 쿼리
            params: 쿼리 파라미터 (? placeholder 사용)

        Returns:
            쿼리 결과 DataFrame

        Raises:
            sqlite3.Error: 쿼리 실행 실패
        """
        try:
            logger.debug(f"SQL 실행: {sql}")
            if params:
                logger.debug(f"파라미터: {params}")

            df = pd.read_sql_query(sql, self.conn, params=params)
            logger.info(f"쿼리 완료: {len(df)} 건")
            return df

        except sqlite3.OperationalError as e:
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
            sqlite3.Error: 쿼리 실행 실패
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
        params = []

        if filters:
            try:
                conditions = []
                for korean_col, value in filters.items():
                    english_col = korean_to_english[korean_col]
                    conditions.append(f"{english_col} = ?")
                    params.append(value)

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
        df = self.query(sql, tuple(params) if params else None)

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
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            )
            result = cursor.fetchone()
            exists = result is not None

            if exists:
                logger.debug(f"테이블 '{table_name}' 존재 확인")
            else:
                logger.debug(f"테이블 '{table_name}' 존재하지 않음")

            return exists

        except sqlite3.Error as e:
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

            cursor = self.conn.cursor()
            cursor.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE ctprvn_nm = ? AND signgu_nm = ?",
                (sido, sigungu),
            )
            count = cursor.fetchone()[0]
            logger.debug(f"{sido} {sigungu} 데이터: {count} 건")
            return count

        except sqlite3.Error as e:
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
            cursor = self.conn.cursor()
            cursor.execute(
                f"DELETE FROM {table_name} WHERE ctprvn_nm = ? AND signgu_nm = ?",
                (sido, sigungu),
            )
            deleted_count = cursor.rowcount
            self.conn.commit()
            logger.info(f"{sido} {sigungu} 데이터 삭제: {deleted_count} 건")
            return deleted_count

        except sqlite3.Error as e:
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
            cursor = self.conn.cursor()

            # 총 레코드 수
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_count = cursor.fetchone()[0]

            # 데이터베이스 파일 크기 (바이트)
            db_size = self.db_path.stat().st_size if self.db_path.exists() else 0
            table_size_mb = db_size / (1024 * 1024)

            # 시도 수
            cursor.execute(f"SELECT COUNT(DISTINCT ctprvn_nm) FROM {table_name}")
            sido_count = cursor.fetchone()[0]

            # 시군구 수
            cursor.execute(f"SELECT COUNT(DISTINCT signgu_nm) FROM {table_name}")
            sigungu_count = cursor.fetchone()[0]

            # 업종 대분류 수
            cursor.execute(f"SELECT COUNT(DISTINCT inds_lcls_nm) FROM {table_name}")
            industry_count = cursor.fetchone()[0]

            stats = {
                "총_레코드_수": total_count,
                "테이블_크기_MB": round(table_size_mb, 2),
                "시도_수": sido_count,
                "시군구_수": sigungu_count,
                "업종대분류_수": industry_count,
            }

            logger.info(f"테이블 통계: {stats}")
            return stats

        except sqlite3.Error as e:
            logger.error(f"통계 조회 실패: {e}")
            raise
