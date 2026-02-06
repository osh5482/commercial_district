# src/collector.py
import pandas as pd
import asyncio
from src.clients import DistrictClient, StoreZoneClient, StoreClient


class Collector:
    def __init__(self):
        self.district_client = DistrictClient()
        self.store_zone_client = StoreZoneClient()
        self.store_client = StoreClient()
        self.semaphore = asyncio.Semaphore(30)

    async def get_sido_code(self, sido_name: str) -> str:
        """시도 이름으로 시도 코드를 조회하는 메서드

        Args:
            sido_name: 조회할 시도 이름 (예: "서울특별시")

        Returns:
            시도 코드 문자열 (예: "11")
        """
        # 1. 행정구역코드 조회: 시도 단위의 행정구역코드를 조회
        try:
            response = await self.district_client.get_districtList(catId="mega")

        except Exception as e:
            raise Exception(f"시도 목록 조회 실패: {e}")

        # 2. json 응답 파싱
        response_body = response.get("body", {})
        response_items = response_body.get("items", [])

        # 3. 시도 이름에 해당하는 시도 코드 반환
        for item in response_items:
            if item.get("ctprvnNm") == sido_name:
                return item.get("ctprvnCd")

        # 시도를 찾지 못한 경우
        raise ValueError(f"시도 '{sido_name}'를 찾을 수 없습니다.")

    async def get_sigungu_code(self, sido_name: str, sigungu_name: str) -> str:
        """시도 이름과 시군구 이름으로 시군구 코드를 조회하는 메서드

        Args:
            sido_name: 조회할 시도 이름 (예: "서울특별시")
            sigungu_name: 조회할 시군구 이름 (예: "강남구")

        Returns:
            시군구 코드 문자열 (예: "680")
        """
        # 1. 시도 코드 조회
        sido_code = await self.get_sido_code(sido_name)

        # 2. 시도 코드로 시군구 목록 조회
        try:
            response = await self.district_client.get_districtList(
                catId="cty", parents_Cd=sido_code
            )

        except Exception as e:
            raise Exception(f"시군구 목록 조회 실패: {e}")

        # 3. json 응답 파싱
        response_body = response.get("body", {})
        response_items = response_body.get("items", [])

        # 4. 시군구 이름에 해당하는 시군구 코드 반환
        for item in response_items:
            if item.get("signguNm") == sigungu_name:
                return item.get("signguCd")

        # 시군구를 찾지 못한 경우
        raise ValueError(f"시군구 '{sigungu_name}'를 찾을 수 없습니다.")

    async def collect_store_zones(self, sido_name: str, sigungu_name: str) -> dict:
        """시도 이름과 시군구 이름으로 상권 데이터를 수집하는 메서드

        Args:
            sido_name: 조회할 시도 이름 (예: "서울특별시")
            sigungu_name: 조회할 시군구 이름 (예: "강남구")

        Returns:
            상권 데이터 리스트 (딕셔너리의 리스트)
        """
        # 1. 시군구 코드 조회
        sigungu_code = await self.get_sigungu_code(sido_name, sigungu_name)

        # 2. 상권 데이터 조회
        try:
            response = await self.store_zone_client.get_storeZoneInAdmi(
                divID="signguCd", district_code=sigungu_code
            )
        except Exception as e:
            raise Exception(f"상권 데이터 조회 실패: {e}")

        # 3. json 응답 파싱
        response_body = response.get("body", {})
        response_items = response_body.get("items", [])

        return response_items

    async def collect_stores(self, sido_name: str, sigungu_name: str) -> pd.DataFrame:
        """시군구 내 모든 상가업소 데이터 수집

        Args:
            sido_name: 조회할 시도 이름 (예: "서울특별시")
            sigungu_name: 조회할 시군구 이름 (예: "강남구")

        Returns:
            상가업소 데이터가 담긴 Pandas DataFrame
        """
        # 1. 시군구 코드 조회
        sigungu_code = await self.get_sigungu_code(sido_name, sigungu_name)

        # 2. 상가정보 조회 (페이징)
        all_items = []
        page_no = 1
        print(f"==== {sido_name} {sigungu_name} 상가업소 데이터 수집 시작 ====")
        while True:
            # 2-1. 상가업소 목록 조회
            try:
                response = await self.store_client.get_storeListInDong(
                    divId="signguCd",
                    district_code=sigungu_code,
                    numOfRows=1000,
                    pageNo=page_no,
                )

            except Exception as e:
                raise Exception(f"상가업소 목록 조회 실패: {e}")

            # 2-2. 응답 데이터 파싱
            response_body = response.get("body", {})
            response_items = response_body.get("items", [])

            # 2-3. 더 이상 데이터가 없으면 종료
            if not response_items:
                break

            # 2-4. 수집된 데이터 누적
            all_items.extend(response_items)
            print(f"수집 중... 페이지 {page_no} 완료, 누적 건수: {len(all_items)}")
            page_no += 1

        # while 끝
        print(
            f"==== {sido_name} {sigungu_name} 상가업소 데이터 수집 완료 (총 {len(all_items)} 건) ===="
        )

        # 3. DataFrame으로 변환하여 반환
        df = pd.DataFrame(all_items)
        return df
