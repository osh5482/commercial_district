# src/clients/district.py
from .base import AsyncBaseAPIClient
from typing import Literal, overload


class DistrictClient(AsyncBaseAPIClient):
    """행정구역코드 조회 전용 클라이언트"""

    @overload
    def get_districtList(self, catId: Literal["mega"], *, parents_Cd: str = None): ...

    @overload
    def get_districtList(self, catId: Literal["cty"], *, parents_Cd: str): ...

    @overload
    def get_districtList(self, catId: Literal["admi"], *, parents_Cd: str): ...

    @overload
    def get_districtList(self, catId: Literal["zone"], *, parents_Cd: str): ...

    async def get_districtList(
        self,
        catId: Literal["mega", "cty", "admi", "zone"],
        *,  # kargs 구분
        parents_Cd: str = None,
    ):
        """행정구역 조회. 시도, 시군구, 행정동 단위의 행정구역코드를 조회하는 기능

        Args:
            resId: 리소스 ID (default: dong, 리소스에 대한 ID로 dong은 행정구역 리소스를 나타낸다.)
            catId: 카테고리 ID (시도: mega, 시군구: cty, 행정동: admi, 법정동: zone)
            parents_Cd: 상위 행정구역코드
                - catId가 mega일 때: 생략 가능
                - catId가 cty일 때: 시도 코드(ctprvnCd) 필수
                - catId가 admi일 때: 시군구 코드(signguCd) 필수

        """
        endpoint = "/baroApi"
        params = {
            "resId": "dong",  # 리소스 ID (default: dong, 리소스에 대한 ID로 dong은 행정구역 리소스를 나타낸다.)
            "catId": catId,
        }

        if parents_Cd is not None:
            if catId == "cty":
                params["ctprvnCd"] = parents_Cd
            elif catId == "admi" or catId == "zone":
                params["signguCd"] = parents_Cd
            else:
                pass

        return await self._make_async_request(endpoint, params)
