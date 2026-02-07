# src/preprocessor.py
import pandas as pd
import numpy as np
from pathlib import Path
from config.logging import logger


class DataPreprocessor:
    """상가업소 데이터 전처리 클래스"""

    def __init__(self):
        """전처리 규칙 초기화"""
        # 숫자형으로 변환할 컬럼
        self.numeric_columns = [
            "lon",  # 경도
            "lat",  # 위도
            "lnoMnno",  # 지번본번지
            "lnoSlno",  # 지본부번지
            "bldMnno",  # 건물본번지
            "bldSlno",  # 건물부번지
            "flrNo",  # 층정보
        ]

        # 필수 컬럼 (항목구분 1)
        self.required_columns = [
            "bizesId",  # 상가업소번호
            "bizesNm",  # 상호명
            "indsLclsCd",  # 상권업종대분류코드
            "indsLclsNm",  # 상권업종대분류명
            "indsMclsCd",  # 상권업종중분류코드
            "indsMclsNm",  # 상권업종중분류명
            "indsSclsCd",  # 상권업종소분류코드
            "indsSclsNm",  # 상권업종소분류명
        ]

        # 한국 좌표 범위 (WGS84 기준)
        self.korea_lon_range = (124.0, 132.0)  # 경도
        self.korea_lat_range = (33.0, 43.0)  # 위도

        logger.info("DataPreprocessor 초기화 완료")

    def _convert_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """컬럼 타입 변환

        Args:
            df: 입력 DataFrame

        Returns:
            타입 변환된 DataFrame
        """
        logger.debug("컬럼 타입 변환 시작")

        for col in self.numeric_columns:
            if col in df.columns:
                # 문자열 → 숫자 변환 (에러는 NaN 처리)
                df[col] = pd.to_numeric(df[col], errors="coerce")
                logger.debug(f"  {col}: {df[col].dtype}")

        logger.debug("컬럼 타입 변환 완료")
        return df

    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """결측치 처리

        Args:
            df: 입력 DataFrame

        Returns:
            결측치 처리된 DataFrame
        """
        logger.debug("결측치 처리 시작")
        before_count = len(df)

        # 필수 컬럼에 결측치가 있는 행 제거
        for col in self.required_columns:
            if col in df.columns:
                missing_count = df[col].isna().sum()
                if missing_count > 0:
                    logger.warning(f"  {col}: {missing_count} 건 결측")
                    df = df.dropna(subset=[col])

        after_count = len(df)
        removed = before_count - after_count
        logger.debug(f"결측치 처리 완료: {removed} 건 제거")

        return df

    def _remove_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """이상치 제거

        Args:
            df: 입력 DataFrame

        Returns:
            이상치 제거된 DataFrame
        """
        logger.debug("이상치 제거 시작")
        before_count = len(df)

        # 1. 좌표 범위 검증
        if "lon" in df.columns and "lat" in df.columns:
            valid_lon = df["lon"].between(*self.korea_lon_range)
            valid_lat = df["lat"].between(*self.korea_lat_range)
            invalid_coord = ~(valid_lon & valid_lat)

            invalid_count = invalid_coord.sum()
            if invalid_count > 0:
                logger.warning(f"  좌표 이상치: {invalid_count} 건")
                df = df[~invalid_coord]

        # 2. 층/번지 음수 값 체크
        for col in ["flrNo", "lnoMnno", "lnoSlno", "bldMnno", "bldSlno"]:
            if col in df.columns:
                negative_count = (df[col] < 0).sum()
                if negative_count > 0:
                    logger.warning(f"  {col} 음수: {negative_count} 건")
                    df = df[df[col] >= 0]

        after_count = len(df)
        removed = before_count - after_count
        logger.debug(f"이상치 제거 완료: {removed} 건 제거")

        return df

    def preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """전체 전처리 파이프라인 실행

        Args:
            df: Raw DataFrame

        Returns:
            전처리된 DataFrame
        """
        logger.info(f"전처리 시작: {len(df)} 건")
        original_count = len(df)

        # 1. 데이터 복사 (원본 보존)
        df_processed = df.copy()

        # 2. 컬럼 타입 변환
        df_processed = self._convert_dtypes(df_processed)

        # 3. 필수 컬럼 결측치 제거
        df_processed = self._handle_missing_values(df_processed)

        # 4. 이상치 제거
        df_processed = self._remove_outliers(df_processed)

        # 5. 인덱스 리셋
        df_processed = df_processed.reset_index(drop=True)

        # 로그 출력
        removed_count = original_count - len(df_processed)
        logger.info(f"전처리 완료: {len(df_processed)} 건 (제거: {removed_count} 건)")

        return df_processed

    def get_summary(self, df: pd.DataFrame) -> dict:
        """전처리된 데이터 요약 정보 반환

        Args:
            df: 전처리된 DataFrame

        Returns:
            요약 정보 딕셔너리
        """
        summary = {
            "총_건수": len(df),
            "시도_수": df["ctprvnNm"].nunique() if "ctprvnNm" in df.columns else 0,
            "시군구_수": df["signguNm"].nunique() if "signguNm" in df.columns else 0,
            "업종대분류_수": (
                df["indsLclsNm"].nunique() if "indsLclsNm" in df.columns else 0
            ),
            "업종중분류_수": (
                df["indsMclsNm"].nunique() if "indsMclsNm" in df.columns else 0
            ),
            "업종소분류_수": (
                df["indsSclsNm"].nunique() if "indsSclsNm" in df.columns else 0
            ),
            "좌표_결측_건수": (
                df[["lon", "lat"]].isna().any(axis=1).sum()
                if "lon" in df.columns
                else 0
            ),
        }
        return summary

    def save_processed(
        self,
        df: pd.DataFrame,
        sido: str,
        sigungu: str,
        base_dir: str = "data/processed",
    ) -> Path:
        """전처리된 데이터를 Parquet로 저장

        Args:
            df: 전처리된 DataFrame
            sido: 시도명
            sigungu: 시군구명
            base_dir: 저장 디렉토리 (기본값: "data/processed")

        Returns:
            저장된 파일 경로
        """
        # 디렉토리 생성
        save_dir = Path(base_dir)
        save_dir.mkdir(parents=True, exist_ok=True)

        # 파일명 생성
        file_name = f"stores_{sido}_{sigungu}_processed.parquet"
        file_path = save_dir / file_name

        # Parquet 저장
        df.to_parquet(file_path, index=False, engine="pyarrow")
        logger.success(f"전처리 데이터 저장 완료: {file_path} ({len(df)} 건)")

        return file_path

    def load_processed(
        self, sido: str, sigungu: str, base_dir: str = "data/processed"
    ) -> pd.DataFrame | None:
        """저장된 전처리 데이터 로드

        Args:
            sido: 시도명
            sigungu: 시군구명
            base_dir: 저장 디렉토리

        Returns:
            전처리된 DataFrame 또는 None
        """
        file_path = Path(base_dir) / f"stores_{sido}_{sigungu}_processed.parquet"

        if not file_path.exists():
            logger.warning(f"전처리 파일 없음: {file_path}")
            return None

        df = pd.read_parquet(file_path, engine="pyarrow")
        logger.info(f"전처리 데이터 로드: {file_path} ({len(df)} 건)")

        return df
