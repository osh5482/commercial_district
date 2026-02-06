# src/clients/store_zone.py
from .base import BaseAPIClient


class StoreZoneClient(BaseAPIClient):
    """상권 조회 전용 클라이언트"""

    def get_storeZoneOne(self, store_zone: str) -> dict | None:
        """지정 상권조회. 전국 약 1200개 주요상권의 영역좌표를 조회하는 기능으로 지정된 상권번호에 해당하는 단일 상권정보를 조회한다.

        Args:
            store_zone: 조회 대상 상권의 상권번호

        Returns:
            JSON 응답을 딕셔너리로 반환
        """
        endpoint = "/storeZoneOne"
        params = {
            "key": store_zone,
        }
        return self._make_request(endpoint, params)

    def get_storeZoneInRadius(self, radius: int, cx: float, cy: float):
        """반경내 상권조회. 전국 약1200개 주요상권의 영역좌표를 조회하는 기능으로 반경영역에 포함되는 상권데이터를 조회할 수 있다.

        Args:
            radius: 반경 (단위: m, 최대 2000m)
            cx: 중심정 경도
            cy: 중심점 위도

        Returns:
            JSON 응답을 딕셔너리로 반환"""
        endpoint = "/storeZoneInRadius"
        params = {"radius": radius, "cx": cx, "cy": cy}
        return self._make_request(endpoint, params)

    def get_storeZoneInRectangle(
        self, minx: float, miny: float, maxx: float, maxy: float
    ):
        """사각형내 상권조회. 전국 약1200개 주요상권의 영역좌표를 조회하는 기능으로 사각형영역에 포함되는 상권데이터를 조회할 수 있다.

        Args:
            minx: 서쪽 경도
            miny: 남쪽 위도
            maxx: 동쪽 경도
            maxy: 북쪽 위도

        Returns:
            JSON 응답을 딕셔너리로 반환"""
        endpoint = "/storeZoneInRectangle"
        params = {"minx": minx, "miny": miny, "maxx": maxx, "maxy": maxy}
        return self._make_request(endpoint, params)

    def get_storeZoneInAdmi(self, divID: str, district_code: str):
        """행정구역 단위 상권조회. 전국 약 1200개 주요상권의 영역좌표를 조회하는 기능으로 중심점 좌표 포함되는 행정구역을 대상으로 상권데이터를 조회할 수 있다.

        Args:
            divID: 구분ID (시도: ctprvnCd, 시군구: signguCd, 행정동: adongCd)
            district_code: 행정구역코드

        Returns:
            JSON 응답을 딕셔너리로 반환"""
        endpoint = "/storeZoneInAdmi"
        params = {"divId": divID, "key": district_code}
        return self._make_request(endpoint, params)
