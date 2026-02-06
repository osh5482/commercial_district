# src/clients/base.py
import aiohttp
import asyncio
from config.settings import API_KEY, BASE_URL


class AsyncBaseAPIClient:
    """소상공인 상가정보 비동기 API 통합 수집 클래스"""

    def __init__(self, api_key: str = API_KEY, base_url: str = BASE_URL):
        """초기화 메서드"""
        self.api_key = api_key
        self.base_url = base_url
        self.session = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """비동기 세션 생성 메서드"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def _make_async_request(self, endpoint: str, params: dict[str, any]) -> dict:
        """
        비동기 API 요청을 보내고 응답을 반환하는 내부 메서드

        Args:
            endpoint: API 엔드포인트 경로
            params: 쿼리 파라미터 딕셔너리

        Returns:
            JSON 응답을 딕셔너리로 반환
        """

        try:
            # 공통 파라미터 추가
            params.update(
                {
                    "serviceKey": self.api_key,  # 인증키
                    "type": "json",  # json 형식
                }
            )
            url = self.base_url + endpoint
            session = await self._get_session()
            async with session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                response.raise_for_status()  # 에러 체크
                return await response.json()

        except aiohttp.ClientResponseError as e:
            # HTTP 에러 (400, 404, 500 등)
            status_code = e.status
            if status_code == 400:
                raise Exception(f"잘못된 요청입니다 (400): {e}")
            elif status_code == 401:
                raise Exception(f"인증 실패 (401): API 키를 확인하세요")
            elif status_code == 404:
                raise Exception(f"리소스를 찾을 수 없습니다 (404): {e}")
            elif status_code == 429:
                raise Exception(f"API 호출 한도 초과 (429): 잠시 후 다시 시도하세요")
            elif status_code >= 500:
                raise Exception(f"서버 오류 ({status_code}): {e}")
            else:
                raise Exception(f"HTTP 에러 ({status_code}): {e}")

        except asyncio.TimeoutError:
            raise Exception("요청 시간 초과 (30초)")

        except aiohttp.ClientConnectorError:
            raise Exception("네트워크 연결 실패")

        except aiohttp.ClientError as e:
            raise Exception(f"API 요청 실패: {e}")

        finally:
            await self.close()

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

    async def close(self):
        """세션 종료 메서드"""
        if self.session and not self.session.closed:
            await self.session.close()
