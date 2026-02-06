# src/clients/upjong.py
from .base import AsyncBaseAPIClient


class UpjongClient(AsyncBaseAPIClient):
    """업종코드 조회 전용 클라이언트"""

    async def get_largeUpjongList(self):
        """상권업종대분류코드 조회. 상권업종대분류코드 목록을 조회

        Returns:
            JSON 응답을 딕셔너리로 반환"""
        endpoint = "/largeUpjongList"
        params = {}
        return await self._make_async_request(endpoint, params)

    async def get_middleUpjongList(self, *, indsLclsCd: str = None):
        """상권업종중분류코드 조회. 상권업종대분류코드에 해당하는 상권업종중분류코드 목록을 조회

        Args:
            indsLclsCd: 상권업종대분류코드 (optional)

        Returns:
            JSON 응답을 딕셔너리로 반환"""
        endpoint = "/middleUpjongList"

        if indsLclsCd is not None:
            params = {
                "indsLclsCd": indsLclsCd,
            }
        else:
            params = {}
        return await self._make_async_request(endpoint, params)

    async def get_smallUpjongList(
        self, *, indsLclsCd: str = None, indsMclsCd: str = None
    ):
        """상권업종소분류코드 조회. 상권업종대분류코드와 중분류코드에 해당하는 상권업종소분류코드 목록을 조회

        Args:
            indsLclsCd: 상권업종대분류코드 (optional)
            indsMclsCd: 상권업종중분류코드 (optional)

        Returns:
            JSON 응답을 딕셔너리로 반환"""
        endpoint = "/smallUpjongList"
        params = {
            "indsLclsCd": indsLclsCd,
            "indsMclsCd": indsMclsCd,
        }
        params = {k: v for k, v in params.items() if v is not None}
        return await self._make_async_request(endpoint, params)
