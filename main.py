from src.clients import StoreZoneClient, StoreClient, UpjongClient, DistrictClient


def main():  # 테스트
    district_client = DistrictClient()
    result = district_client.get_districtList(catId="zone", parents_Cd="11680")
    print(result)
    return


if __name__ == "__main__":
    main()
