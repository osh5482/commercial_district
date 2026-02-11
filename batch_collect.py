#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""배치 데이터 수집 스크립트

여러 지역의 상가업소 데이터를 자동으로 수집→전처리→DB저장까지 일괄 처리합니다.
지역 목록은 API를 통해 동적으로 가져오므로, 하드코딩 없이 전국 어디든 수집 가능합니다.

실행 방법:
    python batch_collect.py                      # 서울시 전체 자동 수집
    python batch_collect.py --sido 부산광역시    # 부산시 전체 자동 수집
    python batch_collect.py --force              # 기존 데이터 덮어쓰기
    python batch_collect.py --skip-existing      # 이미 수집된 구 스킵
"""

import asyncio
import argparse
from datetime import datetime
from typing import List, Tuple, Dict
from src.collector import Collector
from src.clients import DistrictClient
from src.storage import DataStorage
from src.preprocessor import DataPreprocessor
from src.database import DatabaseManager
from config.logging import logger


# ============================================================
# 지역 목록 API 조회 함수
# ============================================================


async def get_all_sido_list() -> List[Dict[str, str]]:
    """전국 시도 목록을 API로 조회

    Returns:
        시도 목록 리스트 [{"ctprvnCd": "11", "ctprvnNm": "서울특별시"}, ...]
    """
    logger.info("📡 API로 전국 시도 목록 조회 중...")

    async with DistrictClient() as client:
        response = await client.get_districtList(catId="mega")
        body = response.get("body", {})
        items = body.get("items", [])

    logger.success(f"✅ 시도 목록 조회 완료: {len(items)} 개")
    for item in items:
        logger.debug(f"  - {item.get('ctprvnNm')} ({item.get('ctprvnCd')})")

    return items


async def get_districts_from_api(sido_name: str) -> List[str]:
    """API를 통해 특정 시도의 시군구 목록을 동적으로 조회

    Args:
        sido_name: 시도명 (예: "서울특별시", "부산광역시")

    Returns:
        시군구명 리스트 (예: ["강남구", "강동구", ...])
    """
    logger.info(f"📡 API로 {sido_name} 시군구 목록 조회 중...")

    try:
        async with DistrictClient() as client:
            # 1. 시도 코드 조회
            sido_response = await client.get_districtList(catId="mega")
            sido_items = sido_response.get("body", {}).get("items", [])

            sido_code = None
            for item in sido_items:
                if item.get("ctprvnNm") == sido_name:
                    sido_code = item.get("ctprvnCd")
                    break

            if not sido_code:
                raise ValueError(f"시도 '{sido_name}'를 찾을 수 없습니다.")

            logger.debug(f"시도 코드: {sido_code}")

            # 2. 시군구 목록 조회
            sigungu_response = await client.get_districtList(
                catId="cty", parents_Cd=sido_code
            )
            sigungu_items = sigungu_response.get("body", {}).get("items", [])

            # 3. 시군구명만 추출
            district_names = [item.get("signguNm") for item in sigungu_items]

            logger.success(
                f"✅ {sido_name} 시군구 목록 조회 완료: {len(district_names)} 개"
            )
            for name in district_names:
                logger.debug(f"  - {name}")

            return district_names

    except Exception as e:
        logger.error(f"시군구 목록 조회 실패: {e}")
        raise


# ============================================================
# 배치 수집 함수
# ============================================================


async def collect_one_district(
    sido: str, sigungu: str, force_update: bool = False
) -> Tuple[bool, int, Dict[str, float]]:
    """한 개 구의 데이터를 수집→전처리→DB저장

    Args:
        sido: 시도명 (예: "서울특별시")
        sigungu: 시군구명 (예: "강남구")
        force_update: True면 기존 데이터를 삭제하고 재수집

    Returns:
        (성공 여부, 저장된 레코드 수, 시간 통계)
        시간 통계: {"collect": 초, "preprocess": 초, "db_save": 초, "total": 초}
    """
    # 전체 시작 시간
    total_start = datetime.now()
    time_stats = {"collect": 0.0, "preprocess": 0.0, "db_save": 0.0, "total": 0.0}

    try:
        logger.info(f"\n{'='*60}")
        logger.info(f"🏙️  처리 시작: {sido} {sigungu}")
        logger.info(f"{'='*60}")

        storage = DataStorage()
        preprocessor = DataPreprocessor()

        # ----------------------------------------
        # 1단계: 데이터 수집 (API 호출)
        # ----------------------------------------
        logger.info(f"\n[1/3] 데이터 수집 중...")
        collect_start = datetime.now()

        # 기존 파일 확인
        if not force_update and storage.file_exists(sido, sigungu):
            logger.info(f"✅ 기존 Raw 데이터 파일 사용")
            df_raw = storage.load_stores(sido, sigungu)
        else:
            logger.info(f"🌐 API 호출하여 데이터 수집")
            async with Collector() as collector:
                df_raw = await collector.collect_stores(sido, sigungu)

            if not df_raw.empty:
                storage.save_stores(df_raw, sido, sigungu, format="parquet")
                logger.success(f"✅ Raw 데이터 저장 완료: {len(df_raw):,} 건")
            else:
                logger.warning(f"⚠️ 수집된 데이터 없음")
                return (False, 0, time_stats)

        collect_end = datetime.now()
        time_stats["collect"] = (collect_end - collect_start).total_seconds()
        logger.info(f"⏱️  수집 소요 시간: {time_stats['collect']:.2f}초")

        # ----------------------------------------
        # 2단계: 데이터 전처리
        # ----------------------------------------
        logger.info(f"\n[2/3] 데이터 전처리 중...")
        preprocess_start = datetime.now()

        df_processed = preprocessor.preprocess(df_raw)

        if df_processed.empty:
            logger.warning(f"⚠️ 전처리 후 데이터 없음")
            return (False, 0, time_stats)

        # 전처리 데이터 저장
        preprocessor.save_processed(df_processed, sido, sigungu)

        preprocess_end = datetime.now()
        time_stats["preprocess"] = (preprocess_end - preprocess_start).total_seconds()

        logger.success(
            f"✅ 전처리 완료: {len(df_raw):,} → {len(df_processed):,} 건 "
            f"(제거: {len(df_raw) - len(df_processed):,})"
        )
        logger.info(f"⏱️  전처리 소요 시간: {time_stats['preprocess']:.2f}초")

        # ----------------------------------------
        # 3단계: PostgreSQL DB에 저장
        # ----------------------------------------
        logger.info(f"\n[3/3] PostgreSQL에 저장 중...")
        db_start = datetime.now()

        with DatabaseManager() as db:
            # 테이블 존재 여부 확인
            table_exists = db.table_exists("stores")

            if not table_exists:
                # 테이블이 없으면 생성
                logger.info("📦 stores 테이블 생성 중...")
                db.create_table_from_metadata(df=df_processed)
                db.create_indexes()
                logger.success("✅ 테이블 생성 완료")

            # 기존 데이터 확인 및 처리
            existing_count = db.get_region_data_count(sido, sigungu)

            if existing_count > 0:
                if force_update:
                    logger.info(f"🔄 기존 데이터 삭제 중: {existing_count:,} 건")
                    db.delete_region_data(sido, sigungu)
                else:
                    logger.warning(
                        f"⚠️ 기존 데이터 존재: {existing_count:,} 건 "
                        f"(--force 옵션으로 덮어쓰기 가능)"
                    )
                    return (False, existing_count, time_stats)

            # 데이터 삽입
            inserted_count = db.insert_dataframe(df_processed, if_exists="append")

        db_end = datetime.now()
        time_stats["db_save"] = (db_end - db_start).total_seconds()

        logger.success(f"✅ DB 저장 완료: {inserted_count:,} 건")
        logger.info(f"⏱️  DB 저장 소요 시간: {time_stats['db_save']:.2f}초")

        # 전체 시간 계산
        total_end = datetime.now()
        time_stats["total"] = (total_end - total_start).total_seconds()

        logger.success(f"\n✅ {sido} {sigungu} 처리 완료!")
        logger.info(f"⏱️  총 소요 시간: {time_stats['total']:.2f}초")

        return (True, inserted_count, time_stats)

    except Exception as e:
        logger.error(f"\n❌ {sido} {sigungu} 처리 실패: {e}")
        logger.exception("상세 에러:")

        # 실패해도 지금까지의 시간은 기록
        total_end = datetime.now()
        time_stats["total"] = (total_end - total_start).total_seconds()

        return (False, 0, time_stats)


async def batch_collect(
    sido: str = "서울특별시",
    districts: List[str] = None,
    force_update: bool = False,
    skip_existing: bool = False,
) -> None:
    """여러 지역의 데이터를 일괄 수집

    Args:
        sido: 시도명 (기본값: "서울특별시")
        districts: 수집할 시군구 목록 (None이면 API로 자동 조회)
        force_update: True면 기존 데이터를 덮어쓰기
        skip_existing: True면 이미 DB에 있는 지역은 스킵
    """
    # districts가 None이면 API로 조회
    if districts is None:
        logger.info(f"\n{'='*60}")
        logger.info(f"🔍 {sido} 시군구 목록을 API에서 조회합니다...")
        logger.info(f"{'='*60}")
        districts = await get_districts_from_api(sido)

    logger.info(f"\n{'='*60}")
    logger.info(f"🚀 배치 수집 시작")
    logger.info(f"{'='*60}")
    logger.info(f"대상 지역: {sido}")
    logger.info(f"수집 구 수: {len(districts)} 개")
    logger.info(f"옵션: force_update={force_update}, skip_existing={skip_existing}")
    logger.info(f"시작 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'='*60}\n")

    # 통계 초기화
    success_count = 0
    fail_count = 0
    skip_count = 0
    total_records = 0

    # 시간 통계 수집용
    all_time_stats = []  # 각 구별 시간 통계
    district_times = []  # 각 구별 (구명, 총 소요시간)

    start_time = datetime.now()

    # 각 구별로 수집
    for i, sigungu in enumerate(districts, 1):
        logger.info(f"\n\n📍 [{i}/{len(districts)}] {sido} {sigungu}")

        # skip_existing 옵션이 True면 기존 데이터 확인
        if skip_existing and not force_update:
            with DatabaseManager() as db:
                existing_count = db.get_region_data_count(sido, sigungu)
                if existing_count > 0:
                    logger.info(f"⏭️  이미 수집됨: {existing_count:,} 건 (스킵)")
                    skip_count += 1
                    total_records += existing_count
                    continue

        # 데이터 수집 및 저장
        success, count, time_stats = await collect_one_district(
            sido, sigungu, force_update
        )

        # 시간 통계 수집
        if time_stats["total"] > 0:
            all_time_stats.append(time_stats)
            district_times.append((sigungu, time_stats["total"]))

        if success:
            success_count += 1
            total_records += count
        else:
            if count > 0:  # 기존 데이터가 있어서 스킵된 경우
                skip_count += 1
                total_records += count
            else:  # 실패한 경우
                fail_count += 1

        # 진행률 출력
        progress = (i / len(districts)) * 100
        logger.info(f"\n📊 진행률: {progress:.1f}% ({i}/{len(districts)})")

    # ============================================================
    # 최종 결과 출력
    # ============================================================
    end_time = datetime.now()
    duration = end_time - start_time

    logger.info(f"\n\n{'='*60}")
    logger.info(f"🎉 배치 수집 완료!")
    logger.info(f"{'='*60}")
    logger.info(f"총 처리 구: {len(districts)} 개")
    logger.info(f"✅ 성공: {success_count} 개")
    logger.info(f"⏭️  스킵: {skip_count} 개")
    logger.info(f"❌ 실패: {fail_count} 개")
    logger.info(f"📦 총 레코드: {total_records:,} 건")
    logger.info(f"⏱️  총 소요 시간: {duration}")
    logger.info(f"종료 시각: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'='*60}\n")

    # ============================================================
    # 시간 통계 분석
    # ============================================================
    if all_time_stats:
        logger.info(f"\n📊 시간 통계 분석")
        logger.info(f"{'='*60}")

        # 단계별 평균 시간 계산
        avg_collect = sum(s["collect"] for s in all_time_stats) / len(all_time_stats)
        avg_preprocess = sum(s["preprocess"] for s in all_time_stats) / len(
            all_time_stats
        )
        avg_db_save = sum(s["db_save"] for s in all_time_stats) / len(all_time_stats)
        avg_total = sum(s["total"] for s in all_time_stats) / len(all_time_stats)

        # 단계별 최소/최대 시간
        min_collect = min(s["collect"] for s in all_time_stats)
        max_collect = max(s["collect"] for s in all_time_stats)
        min_preprocess = min(s["preprocess"] for s in all_time_stats)
        max_preprocess = max(s["preprocess"] for s in all_time_stats)
        min_db_save = min(s["db_save"] for s in all_time_stats)
        max_db_save = max(s["db_save"] for s in all_time_stats)

        logger.info(f"\n[단계별 평균 소요 시간]")
        logger.info(
            f"  1. 데이터 수집:  {avg_collect:>6.2f}초 (최소: {min_collect:.2f}초, 최대: {max_collect:.2f}초)"
        )
        logger.info(
            f"  2. 데이터 전처리: {avg_preprocess:>6.2f}초 (최소: {min_preprocess:.2f}초, 최대: {max_preprocess:.2f}초)"
        )
        logger.info(
            f"  3. DB 저장:     {avg_db_save:>6.2f}초 (최소: {min_db_save:.2f}초, 최대: {max_db_save:.2f}초)"
        )
        logger.info(f"  ─────────────────────────────────")
        logger.info(f"  총 평균:        {avg_total:>6.2f}초")

        # 구별 전체 시간 계산
        total_collect = sum(s["collect"] for s in all_time_stats)
        total_preprocess = sum(s["preprocess"] for s in all_time_stats)
        total_db_save = sum(s["db_save"] for s in all_time_stats)

        logger.info(f"\n[단계별 총 소요 시간]")
        logger.info(
            f"  1. 데이터 수집:  {total_collect:>7.2f}초 ({total_collect/60:>5.1f}분)"
        )
        logger.info(
            f"  2. 데이터 전처리: {total_preprocess:>7.2f}초 ({total_preprocess/60:>5.1f}분)"
        )
        logger.info(
            f"  3. DB 저장:     {total_db_save:>7.2f}초 ({total_db_save/60:>5.1f}분)"
        )

        # 병목 구간 분석
        logger.info(f"\n[병목 구간 분석]")
        total_time = total_collect + total_preprocess + total_db_save
        if total_time > 0:
            collect_pct = (total_collect / total_time) * 100
            preprocess_pct = (total_preprocess / total_time) * 100
            db_save_pct = (total_db_save / total_time) * 100

            logger.info(
                f"  데이터 수집:  {collect_pct:>5.1f}% {'▓' * int(collect_pct/5)}"
            )
            logger.info(
                f"  데이터 전처리: {preprocess_pct:>5.1f}% {'▓' * int(preprocess_pct/5)}"
            )
            logger.info(
                f"  DB 저장:     {db_save_pct:>5.1f}% {'▓' * int(db_save_pct/5)}"
            )

        # 가장 느린 구 Top 5
        if len(district_times) > 0:
            logger.info(f"\n[처리 시간이 긴 구 Top 5]")
            sorted_times = sorted(district_times, key=lambda x: x[1], reverse=True)[:5]
            for rank, (district, time) in enumerate(sorted_times, 1):
                logger.info(
                    f"  {rank}. {district:<10} {time:>6.2f}초 ({time/60:>4.1f}분)"
                )

        logger.info(f"{'='*60}\n")
    else:
        logger.warning("시간 통계 데이터가 없습니다.")

    # DB 최종 통계 조회
    try:
        with DatabaseManager() as db:
            stats = db.get_stats()
            logger.info(f"\n📊 데이터베이스 전체 통계")
            logger.info(f"{'='*60}")
            for key, value in stats.items():
                logger.info(f"{key}: {value:,}")
            logger.info(f"{'='*60}\n")
    except Exception as e:
        logger.warning(f"통계 조회 실패: {e}")


# ============================================================
# CLI 인터페이스
# ============================================================


async def list_sido():
    """전국 시도 목록 조회 (--list-sido 옵션)"""
    logger.info("\n📋 전국 시도 목록\n")
    logger.info("=" * 60)

    items = await get_all_sido_list()

    logger.info(f"\n{'코드':<6} {'시도명':<15}")
    logger.info("-" * 60)
    for item in items:
        code = item.get("ctprvnCd", "")
        name = item.get("ctprvnNm", "")
        logger.info(f"{code:<6} {name:<15}")

    logger.info("=" * 60)
    logger.info(f"총 {len(items)} 개 시도")
    logger.info("\n사용 예시:")
    logger.info("  python batch_collect.py --sido 서울특별시")
    logger.info("  python batch_collect.py --sido 부산광역시")


async def list_districts(sido: str):
    """특정 시도의 시군구 목록 조회 (--list-districts 옵션)"""
    logger.info(f"\n📋 {sido} 시군구 목록\n")
    logger.info("=" * 60)

    districts = await get_districts_from_api(sido)

    logger.info(f"\n번호  시군구명")
    logger.info("-" * 60)
    for i, name in enumerate(districts, 1):
        logger.info(f"{i:>3}.  {name}")

    logger.info("=" * 60)
    logger.info(f"총 {len(districts)} 개 시군구")
    logger.info("\n사용 예시:")
    logger.info(f"  python batch_collect.py --sido {sido}")
    logger.info(f"  python batch_collect.py --sido {sido} --districts 강남구 강동구")


def main():
    """CLI 메인 함수"""
    parser = argparse.ArgumentParser(
        description="상가업소 데이터 배치 수집 스크립트 (API 기반 지역 목록 자동 조회)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  # 서울시 전체 자동 수집 (API로 구 목록 조회)
  python batch_collect.py

  # 부산시 전체 자동 수집
  python batch_collect.py --sido 부산광역시

  # 전국 시도 목록 조회
  python batch_collect.py --list-sido

  # 서울시 구 목록 조회
  python batch_collect.py --list-districts --sido 서울특별시

  # 특정 구만 수집
  python batch_collect.py --districts 강남구 강동구 송파구

  # 기존 데이터 덮어쓰기
  python batch_collect.py --force

  # 이미 수집된 구 스킵
  python batch_collect.py --skip-existing
        """,
    )

    parser.add_argument(
        "--sido", type=str, default="서울특별시", help="시도명 (기본값: 서울특별시)"
    )

    parser.add_argument(
        "--districts", nargs="+", help="수집할 시군구 목록 (미지정 시 API로 자동 조회)"
    )

    parser.add_argument(
        "--force", action="store_true", help="기존 데이터를 삭제하고 재수집"
    )

    parser.add_argument(
        "--skip-existing", action="store_true", help="이미 DB에 있는 지역은 스킵"
    )

    parser.add_argument(
        "--list-sido", action="store_true", help="전국 시도 목록 조회 후 종료"
    )

    parser.add_argument(
        "--list-districts",
        action="store_true",
        help="특정 시도의 시군구 목록 조회 후 종료",
    )

    args = parser.parse_args()

    # --list-sido 옵션: 시도 목록 조회 후 종료
    if args.list_sido:
        asyncio.run(list_sido())
        return

    # --list-districts 옵션: 시군구 목록 조회 후 종료
    if args.list_districts:
        asyncio.run(list_districts(args.sido))
        return

    # 배치 수집 실행
    asyncio.run(
        batch_collect(
            sido=args.sido,
            districts=args.districts,
            force_update=args.force,
            skip_existing=args.skip_existing,
        )
    )


if __name__ == "__main__":
    main()
