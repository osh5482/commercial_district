# src/storage.py
import pandas as pd
from pathlib import Path
from datetime import datetime
from config.logging import logger


class DataStorage:
    """Raw 데이터 저장 및 로드 관리 클래스"""

    def __init__(self, base_dir: str = "data/raw"):
        """
        Args:
            base_dir (str, optional): 기본값 data/raw".
        """
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"DataStorage 초기화: {self.base_dir.absolute()}")

    def save_stores(
        self, df: pd.DataFrame, sido: str, sigungu: str, format: str = "parquet"
    ) -> Path:
        """상가업소 데이터를 파일로 저장

        Args:
            df: 저장할 DataFrame
            sido: 시도명 (예: "서울특별시")
            sigungu: 시군구명 (예: "강남구")
            format: 저장 형식 ("parquet" 또는 "csv")

        Returns:
            저장된 파일 경로
        """
        # 1. 파일명 생성
        timestamp = datetime.now().strftime("%Y%m%d")
        file_name = f"stores_{sido}_{sigungu}_{timestamp}.{format}"
        file_path = self.base_dir / file_name

        # 2. 저장
        if format == "parquet":
            # Parquet 저장 시 모든 컬럼을 문자열로 변환 (타입 충돌 방지)
            df_copy = df.astype(str)
            df_copy.to_parquet(file_path, index=False, engine="pyarrow")
        elif format == "csv":
            df.to_csv(file_path, index=False, encoding="utf-8-sig")
        else:
            raise ValueError(f"지원하지 않는 형식: {format}")
        logger.success(f"데이터 저장 완료: {file_path} ({len(df)} 건)")

        return file_path

    def load_stores(
        self, sido: str, sigungu: str, use_latest: bool = True
    ) -> pd.DataFrame | None:
        """저장된 상가업소 데이터 로드

        Args:
            sido: 시도명
            sigungu: 시군구명
            use_latest: True면 가장 최신 파일 사용, False면 정확한 날짜 매칭 필요

        Returns:
            DataFrame 또는 None (파일이 없을 경우)
        """
        # 1. 패턴 매칭: stores_서울특별시_강남구_{날짜}.parquet
        pattern = f"stores_{sido}_{sigungu}_*"
        files = sorted(self.base_dir.glob(pattern), reverse=True)

        # 데이터 없을 시
        if not files:
            logger.warning(f"저장된 데이터 없음: {sido} {sigungu}")
            return None

        # 2. 가장 최신 파일 사용
        latest_file = files[0]
        logger.info(f"데이터 로드: {latest_file.name}")

        # 3. 확장자에 따라 로드
        if latest_file.suffix == ".parquet":
            df = pd.read_parquet(latest_file, engine="pyarrow")
        elif latest_file.suffix == ".csv":
            df = pd.read_csv(latest_file, encoding="utf-8-sig")
        else:
            raise ValueError(f"지원하지 않는 파일 형식: {latest_file.suffix}")

        logger.info(f"데이터 로드 완료: {latest_file.name} ({len(df)} 건)")
        return df

    def file_exists(self, sido: str, sigungu: str) -> bool:
        """해당 지역 데이터 파일 존재 여부 확인

        Args:
            sido: 시도명
            sigungu: 시군구명

        Returns:
            파일 존재 여부 (bool)
        """
        # 패턴 매칭으로 찾고 bool 반환
        pattern = f"stores_{sido}_{sigungu}_*"
        files = list(self.base_dir.glob(pattern))
        return len(files) > 0

    def list_files(self) -> list[Path]:
        """저장된 모든 데이터 파일 리스트 반환"""
        files = sorted(self.base_dir.glob("stores_*"), reverse=True)
        return files

    def get_file_info(self) -> pd.DataFrame:
        """저장된 파일 정보를 DataFrame으로 반환"""
        files = self.list_files()
        if not files:
            return pd.DataFrame()

        info_list = []
        for file in files:
            # 파일명 파싱: stores_서울특별시_강남구_20250207.parquet
            parts = file.stem.split("_")
            if len(parts) >= 4:
                info_list.append(
                    {
                        "파일명": file.name,
                        "시도": parts[1],
                        "시군구": parts[2],
                        "수집일": parts[3],
                        "크기(MB)": round(file.stat().st_size / 1024 / 1024, 2),
                    }
                )

        return pd.DataFrame(info_list)
