from __future__ import annotations

import os
from pathlib import Path

import streamlit as st

from serving.presentation import format_analysis_rows
from serving.repository import DEFAULT_ANALYSIS_ROW_LIMIT, load_dashboard_data
from serving.snapshot import load_serving_snapshots


DEFAULT_DSN = "postgresql://news_pipeline:news_pipeline_dev@postgres:5432/news_pipeline"


@st.cache_data(ttl=30)
def _load_database(dsn: str, analysis_version: str, row_limit: int) -> dict:
    return load_dashboard_data(
        dsn=dsn,
        analysis_version=analysis_version,
        row_limit=row_limit,
    )


@st.cache_data(ttl=15)
def _load_snapshots(root: str) -> list[dict]:
    return load_serving_snapshots(data_root=Path(root))


def main() -> None:
    st.set_page_config(page_title="News & Comment NLP", page_icon="📊", layout="wide")
    st.title("뉴스·댓글 NLP 파이프라인")
    st.caption("PostgreSQL 최종 분석과 Airflow 실행 snapshot을 읽는 read-only 서빙 화면")

    dsn = os.getenv("POSTGRES_DSN", DEFAULT_DSN)
    snapshot_root = os.getenv("SERVING_SNAPSHOT_ROOT", "data/airflow-output")
    version_labels = {
        "v3 (현재 감정·tone 분석)": "v3",
        "v2 (양극화 분석)": "v2",
        "v1 (초기 감정 분석)": "v1",
        "전체 버전": "all",
    }
    selected_version_label = st.sidebar.selectbox(
        "분석 버전",
        options=list(version_labels),
        index=0,
        help="기본값은 현재 스키마인 v3이며, 과거 분석 결과는 선택해서 확인합니다.",
    )
    analysis_version = version_labels[selected_version_label]
    analysis_row_limit = int(
        st.sidebar.number_input(
            "최근 분석 결과 건수",
            min_value=1,
            max_value=500,
            value=DEFAULT_ANALYSIS_ROW_LIMIT,
            step=10,
            help="최근 분석 결과 표에서 불러올 행 수입니다.",
        )
    )
    if st.button("데이터 새로고침"):
        st.cache_data.clear()

    try:
        data = _load_database(dsn, analysis_version, analysis_row_limit)
    except Exception as error:
        st.error(f"PostgreSQL 결과를 읽지 못했습니다: {type(error).__name__}")
        data = None

    if data is not None:
        st.caption(f"표시 중인 분석 버전: {selected_version_label}")
        summary = data["summary"]
        columns = st.columns(5)
        columns[0].metric("분석 결과", f"{int(summary['analysis_rows']):,}건")
        columns[1].metric("완료 Batch", f"{len(data['batches']):,}건")
        columns[2].metric(
            "평균 감정 점수", f"{float(summary['average_sentiment_score']):.3f}"
        )
        columns[3].metric(
            "평균 양극화", f"{float(summary['average_polarization_score']):.3f}"
        )
        total_cost = sum(
            float(row["total_cost_usd"] or 0) for row in data["batches"]
        )
        columns[4].metric("기록된 비용", f"${total_cost:.6f}")

        left, right = st.columns(2)
        with left:
            st.subheader("감정 분포")
            st.bar_chart(data["sentiments"], x="sentiment", y="row_count")
        with right:
            st.subheader("양극화 분포")
            if data["polarizations"]:
                st.bar_chart(data["polarizations"], x="polarization", y="row_count")
            else:
                st.info("기존 v1 결과에는 양극화 정보가 없습니다.")

        st.subheader("상위 토픽")
        st.bar_chart(data["topics"], x="topic", y="row_count")

        st.subheader("최근 분석 결과")
        st.dataframe(
            format_analysis_rows(data["analyses"]),
            width="stretch",
            hide_index=True,
        )
        with st.expander("Batch 실행 내역"):
            st.dataframe(data["batches"], width="stretch", hide_index=True)

    st.subheader("Airflow end-to-end 실행 snapshot")
    snapshots = _load_snapshots(snapshot_root)
    if not snapshots:
        st.info("아직 serving-snapshot.json이 없습니다. end-to-end DAG를 실행해 주세요.")
    else:
        latest = snapshots[0]
        columns = st.columns(4)
        columns[0].metric("입력", f"{int(latest['input_rows']):,}건")
        columns[1].metric("Spark 고유 저장", f"{int(latest['unique_valid_rows']):,}건")
        columns[2].metric(
            "MinIO 객체", f"{int(latest['object_storage']['object_count']):,}개"
        )
        columns[3].metric("LLM 요청 준비", f"{int(latest['llm']['prepared_rows']):,}건")
        st.json(latest, expanded=False)


if __name__ == "__main__":
    main()
