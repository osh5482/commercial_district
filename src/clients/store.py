# src/clients/store.py
from .base import BaseAPIClient


class StoreClient(BaseAPIClient):
    """상가업소 조회 전용 클라이언트"""

    def get_storeOne(self, store_code: str):
        """5. 단일 상가업소 조회: 상가업소번호에 대한 업소정보를 조회.
            단일 업소정보를 출력이 필요한 경우를 위해 설계된 오퍼레이션

        Args:
            store_code: 상가업소번호

        Returns:
            JSON 응답을 딕셔너리로 반환"""
        endpoint = "/storeOne"
        params = {"key": store_code}
        return self._make_request(endpoint, params)

    def get_storeListInBuilding(
        self,
        store_code: str,
        *,  # kargs 구분자
        indsLclsCd: str = None,
        indsMclsCd: str = None,
        indsSclsCd: str = None,
        numOfRows: int = None,
        pageNo: int = None,
    ):
        """6. 건물 단위 상가업소 조회: 건물관리번호에 대한 업소정보를 조회.
            동일 건물내에 있는 업소목록이 필요한 경우를 위해 설계된 오퍼레이션 [사각형, 다각형 내 상가업소 조회 후 건물관리번호 획득]

        Args:
            store_code: 상가업소번호
            indsLclsCd: 상권업종대분류코드 (optional)
            indsMclsCd: 상권업종중분류코드 (optional)
            indsSclsCd: 상권업종소분류코드 (optional)
            numOfRows: 페이지당 건수 (optional)
            pageNo: 페이지 번호 (optional)

        Returns:
            JSON 응답을 딕셔너리로 반환"""
        endpoint = "/storeListInBuilding"
        params = {
            "key": store_code,
            "indsLclsCd": indsLclsCd,
            "indsMclsCd": indsMclsCd,
            "indsSclsCd": indsSclsCd,
            "numOfRows": numOfRows,
            "pageNo": pageNo,
        }
        params = {k: v for k, v in params.items() if v is not None}

        return self._make_request(endpoint, params)

    def get_storeListInPnu(
        self,
        pnu_code: str,
        *,  # kargs 구분자
        indsLclsCd: str = None,
        indsMclsCd: str = None,
        indsSclsCd: str = None,
        numOfRows: int = None,
        pageNo: int = None,
    ):
        """7. 지번 단위 상가업소 조회: PNU코드에 대한 업소정보를 조회
            동일 지번에 있는 업소목록이 필요한 경우를 위해 설계된 오퍼레이션

        Args:
            pnu_code: 지번 PNU코드
            indsLclsCd: 상권업종대분류코드 (optional)
            indsMclsCd: 상권업종중분류코드 (optional)
            indsSclsCd: 상권업종소분류코드 (optional)
            numOfRows: 페이지당 건수 (optional)
            pageNo: 페이지 번호 (optional)

        Returns:
            JSON 응답을 딕셔너리로 반환"""
        endpoint = "/storeListInPnu"
        params = {
            "key": pnu_code,
            "indsLclsCd": indsLclsCd,
            "indsMclsCd": indsMclsCd,
            "indsSclsCd": indsSclsCd,
            "numOfRows": numOfRows,
            "pageNo": pageNo,
        }
        params = {k: v for k, v in params.items() if v is not None}

        return self._make_request(endpoint, params)

    def get_storeListInDong(
        self,
        divId: str,
        district_code: str,
        *,  # kargs 구분자
        indsLclsCd: str = None,
        indsMclsCd: str = None,
        indsSclsCd: str = None,
        numOfRows: int = None,
        pageNo: int = None,
    ):
        """8. 행정동 단위 상가업소 조회: 행정동코드에 대한 업소정보를 조회
            동일 행정동에 있는 업소목록이 필요한 경우를 위해 설계된 오퍼레이션

        Args:
            divID: 구분ID (시도: ctprvnCd, 시군구: signguCd, 행정동: adongCd)
            district_code: 행정동코드
            indsLclsCd: 상권업종대분류코드 (optional)
            indsMclsCd: 상권업종중분류코드 (optional)
            indsSclsCd: 상권업종소분류코드 (optional)
            numOfRows: 페이지당 건수 (optional)
            pageNo: 페이지 번호 (optional)

        Returns:
            JSON 응답을 딕셔너리로 반환"""
        endpoint = "/storeListInDong"
        params = {
            "divId": divId,
            "key": district_code,
            "indsLclsCd": indsLclsCd,
            "indsMclsCd": indsMclsCd,
            "indsSclsCd": indsSclsCd,
            "numOfRows": numOfRows,
            "pageNo": pageNo,
        }
        params = {k: v for k, v in params.items() if v is not None}

        return self._make_request(endpoint, params)

    def get_storeListInArea(
        self,
        area_code: str,
        *,  # kargs 구분자
        indsLclsCd: str = None,
        indsMclsCd: str = None,
        indsSclsCd: str = None,
        numOfRows: int = None,
        pageNo: int = None,
    ):
        """9. 상권내 상가 업소정보를 조회
            상권번호에 해당하는 업소목록이 필요한 경우를 위해 설계된 오퍼레이션,
            상권에 포함되는 업소에는 상권번호 속성을 가지고 있음. 해당 상권번호와 일치하는 업소 정보를 조회할 수 있다.[상권번호 : 반경내 상권조회 후 상권번호 획득]

        Args:
            area_code: 상권번호
            indsLclsCd: 상권업종대분류코드 (optional)
            indsMclsCd: 상권업종중분류코드 (optional)
            indsSclsCd: 상권업종소분류코드 (optional)
            numOfRows: 페이지당 건수 (optional)
            pageNo: 페이지 번호 (optional)

        Returns:
            JSON 응답을 딕셔너리로 반환"""
        endpoint = "/storeListInArea"
        params = {
            "key": area_code,
            "indsLclsCd": indsLclsCd,
            "indsMclsCd": indsMclsCd,
            "indsSclsCd": indsSclsCd,
            "numOfRows": numOfRows,
            "pageNo": pageNo,
        }
        params = {k: v for k, v in params.items() if v is not None}

        return self._make_request(endpoint, params)

    def get_storeListInRadius(
        self,
        radius: int,
        cx: float,
        cy: float,
        *,  # kargs 구분자
        indsLclsCd: str = None,
        indsMclsCd: str = None,
        indsSclsCd: str = None,
        numOfRows: int = None,
        pageNo: int = None,
    ):
        """10. 반경내 상가업소 조회: 중심점 좌표를 기준으로 반경영역에 포함되는 업소목록을 조회할 수 있다.

        Args:
            radius: 반경 (단위: m, 최대 2000m)
            cx: 중심정 경도
            cy: 중심점 위도
            indsLclsCd: 상권업종대분류코드 (optional)
            indsMclsCd: 상권업종중분류코드 (optional)
            indsSclsCd: 상권업종소분류코드 (optional)
            numOfRows: 페이지당 건수 (optional)
            pageNo: 페이지 번호 (optional)

        Returns:
            JSON 응답을 딕셔너리로 반환"""
        endpoint = "/storeListInRadius"
        params = {
            "radius": radius,
            "cx": cx,
            "cy": cy,
            "indsLclsCd": indsLclsCd,
            "indsMclsCd": indsMclsCd,
            "indsSclsCd": indsSclsCd,
            "numOfRows": numOfRows,
            "pageNo": pageNo,
        }
        params = {k: v for k, v in params.items() if v is not None}

        return self._make_request(endpoint, params)

    def get_storeListInRectangle(
        self,
        minx: float,
        miny: float,
        maxx: float,
        maxy: float,
        *,  # kargs 구분자
        indsLclsCd: str = None,
        indsMclsCd: str = None,
        indsSclsCd: str = None,
        numOfRows: int = None,
        pageNo: int = None,
    ):
        """11. 사각형내 상가 업소정보를 조회
            사각형안에 포함되는 업소목록이 필요한 경우를 위해 설계된 오퍼레이션.
            업소의 경도, 위도 좌표가 입력된 사각형 바운더리 안에 포함되는 업소 정보를 조회할 수 있다.

        Args:
            minx: 서쪽 경도
            miny: 남쪽 위도
            maxx: 동쪽 경도
            maxy: 북쪽 위도
            indsLclsCd: 상권업종대분류코드 (optional)
            indsMclsCd: 상권업종중분류코드 (optional)
            indsSclsCd: 상권업종소분류코드 (optional)
            numOfRows: 페이지당 건수 (optional)
            pageNo: 페이지 번호 (optional)

        Returns:
            JSON 응답을 딕셔너리로 반환"""
        endpoint = "/storeListInRectangle"
        params = {
            "minx": minx,
            "miny": miny,
            "maxx": maxx,
            "maxy": maxy,
            "indsLclsCd": indsLclsCd,
            "indsMclsCd": indsMclsCd,
            "indsSclsCd": indsSclsCd,
            "numOfRows": numOfRows,
            "pageNo": pageNo,
        }
        params = {k: v for k, v in params.items() if v is not None}

        return self._make_request(endpoint, params)

    def get_storeListInPolygon(
        self,
        coordinates: list[tuple[float, float]] | str,
        *,  # kargs 구분자
        indsLclsCd: str = None,
        indsMclsCd: str = None,
        indsSclsCd: str = None,
        numOfRows: int = None,
        pageNo: int = None,
    ):
        """12. 다각형내 상가 업소정보를 조회
            다각형안에 포함되는 업소목록이 필요한 경우를 위해 설계된 오퍼레이션.
            업소의 경도, 위도 좌표가 입력된 다각형 바운더리 안에 포함되는 업소 정보를 조회할 수 있다.

        Args:
            coordinates: 다각형 좌표
                - 리스트 형식: [(경도, 위도), (경도, 위도), ...]
                - WKT 문자열: "POLYGON((경도 위도, 경도 위도, ...))"
            indsLclsCd: 상권업종대분류코드 (optional)
            indsMclsCd: 상권업종중분류코드 (optional)
            indsSclsCd: 상권업종소분류코드 (optional)
            numOfRows: 페이지당 건수 (optional)
            pageNo: 페이지 번호 (optional)

        Returns:
            JSON 응답을 딕셔너리로 반환

        Examples:
            >>> # 방법 1: 좌표 리스트 (권장)
            >>> collector.get_storeListInPolygon([
            ...     (127.0, 37.5),
            ...     (127.1, 37.5),
            ...     (127.1, 37.6),
            ...     (127.0, 37.6),
            ...     (127.0, 37.5)  # 시작점으로 닫아야 함
            ... ])

            >>> # 방법 2: WKT 문자열 직접 입력
            >>> collector.get_storeListInPolygon(
            ...     "POLYGON((127.0 37.5, 127.1 37.5, 127.1 37.6, 127.0 37.5))"
            ... )"""
        endpoint = "/storeListInPolygon"

        # 좌표 리스트를 WKT로 변환

        if isinstance(coordinates, list):
            wkt_key = self._coords_to_wkt(coordinates)
        else:
            wkt_key = coordinates

        params = {
            "key": wkt_key,
            "indsLclsCd": indsLclsCd,
            "indsMclsCd": indsMclsCd,
            "indsSclsCd": indsSclsCd,
            "numOfRows": numOfRows,
            "pageNo": pageNo,
        }
        params = {k: v for k, v in params.items() if v is not None}

        return self._make_request(endpoint, params)

    def get_storeListInUpjong(
        self,
        divId: str,
        upjong_code: str,
        *,  # kargs 구분자
        numOfRows: int = None,
        pageNo: int = None,
    ):
        """13. 업종 단위 상가업소 조회: 업종코드에 대한 업소정보를 조회
            동일 업종에 있는 업소목록이 필요한 경우를 위해 설계된 오퍼레이션

        Args:
            divId: 구분ID (대분류: indsLclsCd, 중분류: indsMclsCd, 소분류: indsSclsCd)
            upjong_code: 업종코드
            numOfRows: 페이지당 건수 (optional)
            pageNo: 페이지 번호 (optional)

        Returns:
            JSON 응답을 딕셔너리로 반환"""
        endpoint = "/storeListInUpjong"
        params = {
            "divId": divId,
            "key": upjong_code,
            "numOfRows": numOfRows,
            "pageNo": pageNo,
        }
        params = {k: v for k, v in params.items() if v is not None}

        return self._make_request(endpoint, params)

    def get_storeListByDate(
        self,
        date: str,
        *,  # kargs 구분자
        indsLclsCd: str = None,
        indsMclsCd: str = None,
        indsSclsCd: str = None,
        numOfRows: int = None,
        pageNo: int = None,
    ):
        """14. 수정일자기준 상가업소 조회: 해당 수정일자를 기준으로 상가 업소정보를 조회
        수정일자를 기준으로 신규, 수정 업소목록이 필요한 경우를 위해 설계된 오퍼레이션 (제공기준 : 영업중인 상가 업소정보)

        Args:
            date: 수정일자 (형식: YYYYMMDD)
            indsLclsCd: 상권업종대분류코드 (optional)
            indsMclsCd: 상권업종중분류코드 (optional)
            indsSclsCd: 상권업종소분류코드 (optional)
            numOfRows: 페이지당 건수 (optional)
            pageNo: 페이지 번호 (optional)

        Returns:
            JSON 응답을 딕셔너리로 반환"""
        endpoint = "/storeListByDate"
        params = {
            "key": date,
            "indsLclsCd": indsLclsCd,
            "indsMclsCd": indsMclsCd,
            "indsSclsCd": indsSclsCd,
            "numOfRows": numOfRows,
            "pageNo": pageNo,
        }
        params = {k: v for k, v in params.items() if v is not None}

        return self._make_request(endpoint, params)
