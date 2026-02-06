# src/clients/base.py
import requests
from config.settings import API_KEY, BASE_URL


class BaseAPIClient:
    """소상공인 상가정보 API 통합 수집 클래스"""

    def __init__(self, api_key: str = API_KEY, base_url: str = BASE_URL):
        """초기화 메서드"""
        self.api_key = api_key
        self.base_url = base_url
        self.type = "json"
        self.session = requests.Session()

    def _make_request(self, endpoint: str, params: dict[str, any]) -> dict:
        """
        API 요청을 보내고 응답을 반환하는 내부 메서드

        Args:
            endpoint: API 엔드포인트 경로
            params: 쿼리 파라미터 딕셔너리

        Returns:
            JSON 응답을 딕셔너리로 반환, 실패 시 None
        """
        try:
            # 공통 파라미터 추가
            params.update(
                {"serviceKey": self.api_key, "type": self.type}  # 인증키  # json 형식
            )
            url = self.base_url + endpoint
            response = self.session.get(url, params=params, timeout=30)
            print(f"요청 URL: {response.url}")  # 디버깅용 요청 URL 출력
            response.raise_for_status()  # 에러 체크
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API 요청 실패: {e}")
            return None  # 또는 예외 재발생

    def _coords_to_wkt(self, coords: list[tuple[float, float]]) -> str:
        """좌표 리스트를 WKT POLYGON 문자열로 변환 (get_storeListInPolygon 에서 사용)

        Args:
            coords: [(경도, 위도), ...] 형식의 좌표 리스트

        Returns:
            "POLYGON((경도 위도, 경도 위도, ...))" 형식 문자열
        """
        if len(coords) < 3:
            raise ValueError("다각형은 최소 3개의 좌표가 필요합니다")

        # 첫 점과 마지막 점이 같지 않으면 자동으로 닫기
        if coords[0] != coords[-1]:
            coords = coords + [coords[0]]

        # WKT 형식으로 변환
        coord_str = ", ".join([f"{lon} {lat}" for lon, lat in coords])
        return f"POLYGON(({coord_str}))"

    def close(self):
        """세션 종료 메서드"""
        self.session.close()
