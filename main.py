from src.clients import StoreZoneClient, StoreClient, UpjongClient, DistrictClient
import json


def print_json(json_data):
    """JSON 데이터를 가독성 좋게 출력하는 유틸리티 함수"""
    print(json.dumps(json_data, indent=2, ensure_ascii=False))


def main():
    """테스트용 메인 함수"""
    store_zone_client = StoreZoneClient()
    result = store_zone_client.get_storeZoneOne(10000, "2025")
    print_json(result)
    return


if __name__ == "__main__":
    main()
