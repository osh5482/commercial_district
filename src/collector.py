# src/collector.py
import pandas as pd
import asyncio
from src.clients import DistrictClient, StoreZoneClient, StoreClient


class Collector:
    def __init__(self):
        self.district_client = DistrictClient()
        self.store_zone_client = StoreZoneClient()
        self.store_client = StoreClient()
        self.semaphore = asyncio.Semaphore(5)  # 동시 요청

    async def __aenter__(self):
        await self.district_client.__aenter__()
        await self.store_zone_client.__aenter__()
        await self.store_client.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.district_client.__aexit__(exc_type, exc_value, traceback)
        await self.store_zone_client.__aexit__(exc_type, exc_value, traceback)
        await self.store_client.__aexit__(exc_type, exc_value, traceback)

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

        # 2. 첫번째 페이지 호출 -> 전체 건수 확인
        first_response = await self.store_client.get_storeListInDong(
            divId="signguCd", district_code=sigungu_code, numOfRows=1000, pageNo=1
        )
        first_body = first_response.get("body", {})
        total_count = first_body.get("totalCount", 0)
        if total_count == 0:
            print(f"==== {sido_name} {sigungu_name} 상가업소 데이터가 없습니다. ====")

        # 2-2. 전체 페이지 수 계산 (예: 2500개면 3페이지)
        total_pages = (total_count // 1000) + (1 if total_count % 1000 > 0 else 0)

        # 3. 비동기 내부 함수 정의
        async def fetch_page(page_no: int) -> list:
            async with self.semaphore:
                try:
                    response = await self.store_client.get_storeListInDong(
                        divId="signguCd",
                        district_code=sigungu_code,
                        numOfRows=1000,
                        pageNo=page_no,
                    )
                    items = response.get("body", {}).get("items", [])
                    print(f"수집중... 페이지 {page_no}/{total_pages} 완료")
                    return items

                except Exception as e:
                    raise Exception(f"상가업소 목록 조회 실패 (페이지 {page_no}): {e}")

        # 4. 2 페이지 부터 마지막 페이지까지 비동기 수집
        tasks = [fetch_page(page_no) for page_no in range(2, total_pages + 1)]

        # 5. 비동기 작업 실행
        print(f"총 {total_pages} 페이지, {total_count} 건 수집 시작...")
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 6. 첫 페이지 데이터와 병합
        all_items = first_body.get("items", [])
        for result in results:
            if isinstance(result, Exception):
                print(f"오류 발생: {result}")
            else:
                all_items.extend(result)

        # 7. Pandas DataFrame 변환
        df = pd.DataFrame(all_items)
        return df
