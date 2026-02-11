# streamlit_app.py
"""상권 분석 대시보드

상가업소 및 상권 데이터를 분석하기 위한 인터랙티브 대시보드.
사용자는 지역, 업종, 키워드로 필터링하여 차트와 지도를 통해 데이터를 탐색할 수 있습니다.
"""

import streamlit as st
from streamlit_folium import st_folium
import folium
from folium.plugins import HeatMap, Fullscreen
import pandas as pd
import plotly.express as px
from src.database import DatabaseManager
from config.logging import logger


# 페이지 설정
st.set_page_config(
    page_title="상권 분석 대시보드",
    page_icon="🏪",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_data
def load_all_data():
    """데이터베이스에서 전체 상가업소 데이터를 로드하는 함수 (성능 최적화를 위해 캐싱)

    Returns:
        pd.DataFrame: 데이터베이스의 전체 상가업소 레코드
    """
    try:
        # 1. DatabaseManager를 통해 DB 연결
        with DatabaseManager() as db:
            # 2. stores 테이블의 전체 데이터 조회
            sql = "SELECT * FROM stores"
            df = db.query(sql)

        # 3. 로그 기록 및 데이터 반환
        logger.info(f"데이터베이스에서 {len(df)} 건의 레코드 로드 완료")
        return df

    except Exception as e:
        # 4. 오류 발생 시 로그 기록 및 에러 메시지 표시
        logger.error(f"데이터 로드 실패: {e}")
        st.error(f"데이터 로드 실패: {e}")
        return pd.DataFrame()


@st.cache_data
def get_filter_options(df: pd.DataFrame):
    """필터 드롭다운에 사용할 고유값 추출

    Args:
        df: 원본 데이터프레임

    Returns:
        dict: 필터 옵션을 담은 딕셔너리
            - sigungus: 시군구 목록
            - dongs: 행정동 목록
            - industries_large: 업종 대분류 목록
            - industries_medium: 업종 중분류 목록
    """
    # 1. 각 컬럼에서 고유값 추출 (결측치 제외)
    sigungus = sorted(df["signgu_nm"].dropna().unique().tolist())
    dongs = sorted(df["adong_nm"].dropna().unique().tolist())
    industries_large = sorted(df["inds_lcls_nm"].dropna().unique().tolist())
    industries_medium = sorted(df["inds_mcls_nm"].dropna().unique().tolist())

    # 2. 딕셔너리로 반환
    return {
        "sigungus": sigungus,
        "dongs": dongs,
        "industries_large": industries_large,
        "industries_medium": industries_medium,
    }


def filter_data(
    df: pd.DataFrame,
    selected_sigungu: str,
    selected_dong: str,
    selected_industry_large: str,
    selected_industry_medium: str,
    keyword: str,
) -> pd.DataFrame:
    """사용자가 선택한 필터 조건에 따라 데이터프레임을 필터링

    Args:
        df: 원본 데이터프레임
        selected_sigungu: 선택된 시군구 (또는 "전체")
        selected_dong: 선택된 행정동 (또는 "전체")
        selected_industry_large: 선택된 업종 대분류 (또는 "전체")
        selected_industry_medium: 선택된 업종 중분류 (또는 "전체")
        keyword: 상호명 검색 키워드

    Returns:
        pd.DataFrame: 필터링된 데이터프레임
    """
    # 1. 원본 데이터 복사 (원본 보존)
    filtered_df = df.copy()

    # 2. 시군구 필터 적용
    if selected_sigungu != "전체":
        filtered_df = filtered_df[filtered_df["signgu_nm"] == selected_sigungu]

    # 3. 행정동 필터 적용
    if selected_dong != "전체":
        filtered_df = filtered_df[filtered_df["adong_nm"] == selected_dong]

    # 4. 업종 대분류 필터 적용
    if selected_industry_large != "전체":
        filtered_df = filtered_df[
            filtered_df["inds_lcls_nm"] == selected_industry_large
        ]

    # 5. 업종 중분류 필터 적용
    if selected_industry_medium != "전체":
        filtered_df = filtered_df[
            filtered_df["inds_mcls_nm"] == selected_industry_medium
        ]

    # 6. 키워드 필터 적용 (상호명 부분 일치 검색, 대소문자 무시)
    if keyword:
        filtered_df = filtered_df[
            filtered_df["bizes_nm"].str.contains(keyword, case=False, na=False)
        ]

    # 7. 필터링된 데이터 반환
    return filtered_df


def create_map(
    df: pd.DataFrame,
    center_lat: float = 37.5,
    center_lon: float = 127.05,
):
    """상가업소 위치를 히트맵으로 표시하는 인터랙티브 Folium 지도 생성

    Args:
        df: 상가업소 데이터프레임 (lat, lon 컬럼 포함)
        center_lat: 지도 중심 위도 (기본값: 37.5)
        center_lon: 지도 중심 경도 (기본값: 127.05)

    Returns:
        folium.Map: 인터랙티브 히트맵 지도 객체
    """
    # 1. 기본 지도 생성 (OpenStreetMap 타일 사용)
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=13,
        tiles="OpenStreetMap",
    )

    # 2. 전체화면 버튼 추가
    Fullscreen(
        position="topright",
        title="전체화면으로 보기",
        title_cancel="전체화면 종료",
        force_separate_button=True,
    ).add_to(m)

    # 3. 좌표가 있는 데이터만 필터링 (모든 데이터 사용)
    df_map = df[df["lat"].notna() & df["lon"].notna()]

    # 4. 히트맵 데이터 생성 (위도, 경도 리스트)
    heat_data = [[row["lat"], row["lon"]] for _, row in df_map.iterrows()]

    # 5. 히트맵 레이어 추가
    HeatMap(
        heat_data,
        min_opacity=0.2,
        radius=15,
        blur=20,
        gradient={0.3: "blue", 0.4: "lime", 0.7: "yellow", 1.0: "red"},
    ).add_to(m)

    # 6. 완성된 지도 객체 반환
    return m


def main():
    """메인 대시보드 애플리케이션"""

    # ========================================
    # 1. 페이지 제목 및 구분선
    # ========================================
    st.title("🏪 상권 분석 대시보드")
    st.markdown("---")

    # ========================================
    # 2. 데이터 로드
    # ========================================
    df = load_all_data()

    # 2-1. 데이터가 없으면 경고 메시지 표시 후 종료
    if df.empty:
        st.warning("데이터가 없습니다. 먼저 데이터를 수집하고 DB에 저장하세요.")
        return

    # 2-2. 필터 옵션 추출
    filter_options = get_filter_options(df)

    # ========================================
    # 3. 사이드바 - 필터 UI 구성
    # ========================================
    st.sidebar.header("🔍 필터")

    # 3-1. 시군구 선택 드롭다운
    selected_sigungu = st.sidebar.selectbox(
        "🏙️ 시군구 선택",
        options=["전체"] + filter_options["sigungus"],
        index=0,
    )

    # 3-2. 행정동 선택 드롭다운 (시군구에 따라 동적으로 필터링)
    if selected_sigungu != "전체":
        # 시군구가 선택된 경우, 해당 시군구에 속하는 행정동만 표시
        dong_options = sorted(
            df[df["signgu_nm"] == selected_sigungu]["adong_nm"]
            .dropna()
            .unique()
            .tolist()
        )
    else:
        # 시군구가 "전체"인 경우, 모든 행정동 표시
        dong_options = filter_options["dongs"]

    selected_dong = st.sidebar.selectbox(
        "📍 행정동 선택",
        options=["전체"] + dong_options,
        index=0,
    )

    # 3-3. 업종 대분류 선택 드롭다운
    selected_industry_large = st.sidebar.selectbox(
        "🏢 업종 대분류",
        options=["전체"] + filter_options["industries_large"],
        index=0,
    )

    # 3-4. 업종 중분류 선택 드롭다운 (대분류에 따라 동적으로 필터링)
    if selected_industry_large != "전체":
        # 대분류가 선택된 경우, 해당 대분류에 속하는 중분류만 표시
        medium_options = sorted(
            df[df["inds_lcls_nm"] == selected_industry_large]["inds_mcls_nm"]
            .dropna()
            .unique()
            .tolist()
        )
    else:
        # 대분류가 "전체"인 경우, 모든 중분류 표시
        medium_options = filter_options["industries_medium"]

    selected_industry_medium = st.sidebar.selectbox(
        "🏪 업종 중분류",
        options=["전체"] + medium_options,
        index=0,
    )

    # 3-5. 키워드 검색 텍스트 입력
    keyword = st.sidebar.text_input("🔎 키워드 검색 (상호명)", value="")

    # 3-6. 사이드바 하단에 전체 데이터 건수 표시
    st.sidebar.markdown("---")
    st.sidebar.info(f"전체 데이터: {len(df):,} 건")

    # ========================================
    # 4. 필터 적용
    # ========================================
    filtered_df = filter_data(
        df,
        selected_sigungu,
        selected_dong,
        selected_industry_large,
        selected_industry_medium,
        keyword,
    )

    # ========================================
    # 5. KPI 메트릭 카드
    # ========================================
    st.subheader("📊 주요 지표")

    # 5-1. 5개의 컬럼으로 레이아웃 구성
    col1, col2, col3, col4, col5 = st.columns(5)

    # 5-2. 총 점포 수
    with col1:
        st.metric(label="총 점포 수", value=f"{len(filtered_df):,} 건")

    # 5-3. 시군구 수
    with col2:
        unique_sigungus = filtered_df["signgu_nm"].nunique()
        st.metric(label="시군구 수", value=f"{unique_sigungus} 개")

    # 5-4. 행정동 수
    with col3:
        unique_dongs = filtered_df["adong_nm"].nunique()
        st.metric(label="행정동 수", value=f"{unique_dongs} 개")

    # 5-5. 업종 중분류 수
    with col4:
        unique_industries = filtered_df["inds_mcls_nm"].nunique()
        st.metric(label="업종 중분류 수", value=f"{unique_industries} 개")

    # 5-6. 좌표 보유율 (지도 표시 가능한 데이터 비율)
    with col5:
        has_coords = filtered_df[
            filtered_df["lat"].notna() & filtered_df["lon"].notna()
        ]
        coord_ratio = (
            len(has_coords) / len(filtered_df) * 100 if len(filtered_df) > 0 else 0
        )
        st.metric(label="좌표 보유율", value=f"{coord_ratio:.1f}%")

    st.markdown("---")

    # ========================================
    # 6. 차트 섹션
    # ========================================
    st.subheader("📈 업종별 분포")

    # 6-1. 2개의 컬럼으로 차트 배치
    chart_col1, chart_col2 = st.columns(2)

    # 6-2. 업종 중분류별 점포 수 차트 (Top 10)
    with chart_col1:
        if not filtered_df.empty:
            # 6-2-1. 업종별 점포 수 집계 (상위 10개)
            industry_counts = (
                filtered_df["inds_mcls_nm"].value_counts().head(10).reset_index()
            )
            industry_counts.columns = ["업종", "점포 수"]

            # 6-2-2. Plotly 가로 막대 차트 생성 (내림차순 정렬: 가장 많은 게 위에)
            fig1 = px.bar(
                industry_counts,
                x="점포 수",
                y="업종",
                orientation="h",
                title="업종 중분류별 점포 수 (Top 10)",
                color="점포 수",
                color_continuous_scale="Blues",
            )
            fig1.update_layout(showlegend=False, height=400)
            fig1.update_yaxes(
                categoryorder="total ascending"
            )  # 내림차순 정렬 (위에서 아래로)

            # 6-2-3. 차트 표시
            st.plotly_chart(fig1, width="stretch")
        else:
            st.info("필터 조건에 맞는 데이터가 없습니다.")

    # 6-3. 행정동별 점포 수 차트 (Top 10)
    with chart_col2:
        if not filtered_df.empty:
            # 6-3-1. 행정동별 점포 수 집계 (상위 10개)
            dong_counts = filtered_df["adong_nm"].value_counts().head(10).reset_index()
            dong_counts.columns = ["행정동", "점포 수"]

            # 6-3-2. Plotly 가로 막대 차트 생성 (내림차순 정렬: 가장 많은 게 위에)
            fig2 = px.bar(
                dong_counts,
                x="점포 수",
                y="행정동",
                orientation="h",
                title="행정동별 점포 수 (Top 10)",
                color="점포 수",
                color_continuous_scale="Greens",
            )
            fig2.update_layout(showlegend=False, height=400)
            fig2.update_yaxes(
                categoryorder="total ascending"
            )  # 내림차순 정렬 (위에서 아래로)

            # 6-3-3. 차트 표시
            st.plotly_chart(fig2, width="stretch")
        else:
            st.info("필터 조건에 맞는 데이터가 없습니다.")

    st.markdown("---")

    # ========================================
    # 7. 인터랙티브 지도 섹션
    # ========================================
    st.subheader("🗺️ 인터랙티브 지도")

    if not filtered_df.empty:
        # 7-1. 좌표가 있는 데이터만 필터링
        has_coords = filtered_df[
            filtered_df["lat"].notna() & filtered_df["lon"].notna()
        ]

        if not has_coords.empty:
            # 7-2. 필터링된 데이터의 중심 좌표 계산 (지도 중앙)
            center_lat = has_coords["lat"].mean()
            center_lon = has_coords["lon"].mean()

            # 7-3. Folium 히트맵 지도 생성 (모든 데이터 표시)
            map_obj = create_map(filtered_df, center_lat, center_lon)

            # 7-4. Streamlit에 지도 표시 (높이 500px)
            # returned_objects: 지도 상호작용 시 반환되는 객체 목록 (빈 리스트로 설정하여 리렌더링 최소화)
            st_folium(map_obj, width=None, height=500, returned_objects=[])

            # 7-5. 지도 데이터 정보 표시
            st.caption(f"💡 히트맵에 표시된 점포: {len(has_coords):,} 개 (모든 데이터)")
        else:
            # 7-6. 좌표가 없는 경우 경고 메시지
            st.warning("좌표 정보가 없는 데이터입니다.")
    else:
        # 7-7. 필터링된 데이터가 없는 경우 안내 메시지
        st.info("필터 조건에 맞는 데이터가 없습니다.")

    st.markdown("---")

    # ========================================
    # 8. 데이터 테이블 (확장 가능)
    # ========================================
    with st.expander("📋 데이터 테이블 보기"):
        if not filtered_df.empty:
            # 8-1. 표시할 주요 컬럼 선택
            display_cols = [
                "bizes_nm",
                "inds_lcls_nm",
                "inds_mcls_nm",
                "adong_nm",
                "rdnm_adr",
            ]
            # 8-2. 컬럼이 실제로 존재하는지 확인 (방어적 프로그래밍)
            display_cols = [col for col in display_cols if col in filtered_df.columns]

            # 8-3. 데이터프레임 표시 (최대 100건)
            st.dataframe(
                filtered_df[display_cols].head(100),
                width="stretch",
                height=300,
            )

            # 8-4. 표시된 데이터 건수 안내
            st.caption(
                f"표시된 데이터: {min(len(filtered_df), 100):,} / {len(filtered_df):,} 건"
            )
        else:
            # 8-5. 표시할 데이터가 없는 경우
            st.info("표시할 데이터가 없습니다.")


if __name__ == "__main__":
    main()
