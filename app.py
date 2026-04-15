"""
日阪製作所 Salesforce活用デモダッシュボード
==========================================
Lepont + Streamlit で「現場が使えるSalesforce」のイメージを提供
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# ── Page Config ──
st.set_page_config(
    page_title="日阪製作所 営業ダッシュボード",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Password Gate (Cloud deployment) ──
def check_password():
    """Simple password gate for Streamlit Cloud."""
    try:
        valid_passwords = st.secrets["auth"]["passwords"]
    except Exception:
        return True  # No auth config = local dev, skip

    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if st.session_state.authenticated:
        return True

    st.markdown("""
    <div style="display:flex;justify-content:center;align-items:center;min-height:60vh">
        <div style="text-align:center">
            <div style="font-size:48px;margin-bottom:16px">🏭</div>
            <div style="font-size:28px;font-weight:800;color:#F8FAFC;margin-bottom:8px">HISAKA Dashboard</div>
            <div style="font-size:14px;color:#94A3B8;margin-bottom:32px">アクセスにはパスワードが必要です</div>
        </div>
    </div>""", unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        password = st.text_input("パスワード", type="password", key="pwd_input")
        if st.button("ログイン", use_container_width=True, type="primary"):
            if password in valid_passwords:
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.error("パスワードが正しくありません")
    return False


if not check_password():
    st.stop()

# ── Design System ──
COLORS = {
    "bg": "#0F172A",
    "card": "#1E293B",
    "card_hover": "#334155",
    "text": "#F8FAFC",
    "text_sub": "#94A3B8",
    "accent": "#FF6B35",
    "blue": "#3B82F6",
    "cyan": "#06B6D4",
    "green": "#22C55E",
    "red": "#EF4444",
    "yellow": "#F59E0B",
    "purple": "#8B5CF6",
    "surface": "#1E293B",
}
SEG_COLORS = {"熱交換器事業": "#3B82F6", "PE事業": "#22C55E", "バルブ事業": "#FF6B35"}
GRADIENT_BLUE = ["#1E3A5F", "#2563EB", "#3B82F6", "#60A5FA", "#93C5FD", "#BFDBFE"]
GRADIENT_WARM = ["#FF6B35", "#FB923C", "#FDBA74"]

# Plotly global template
PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter, Helvetica Neue, Arial, sans-serif", color="#F8FAFC", size=13),
    margin=dict(l=20, r=20, t=40, b=20),
    hoverlabel=dict(bgcolor="#334155", font_size=13, font_color="#F8FAFC", bordercolor="rgba(0,0,0,0)"),
    xaxis=dict(showgrid=False, zeroline=False, showline=False),
    yaxis=dict(showgrid=True, gridcolor="rgba(148,163,184,0.1)", zeroline=False, showline=False),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=12)),
)

# ── Custom CSS ──
st.markdown("""
<style>
    /* Dark theme override */
    .stApp { background-color: #0F172A; }
    .stTabs [data-baseweb="tab-list"] { gap: 0px; background-color: #1E293B; border-radius: 12px; padding: 4px; }
    .stTabs [data-baseweb="tab"] { background-color: transparent; color: #94A3B8; border-radius: 8px; padding: 8px 20px; font-weight: 500; }
    .stTabs [aria-selected="true"] { background: linear-gradient(135deg, #FF6B35, #FB923C); color: white !important; }
    .stSidebar { background-color: #1E293B; }
    .stSidebar .stMarkdown { color: #F8FAFC; }
    .stDivider { border-color: rgba(148,163,184,0.15) !important; }
    h1, h2, h3, h4 { color: #F8FAFC !important; }
    .stDataFrame { border-radius: 12px; overflow: hidden; }
    .stAlert { border-radius: 12px; }
    [data-testid="stMetricValue"] { color: #F8FAFC; }

    /* Chat / AI analysis text */
    [data-testid="stChatMessage"] { background-color: #1E293B !important; border: 1px solid rgba(148,163,184,0.15); border-radius: 12px; }
    [data-testid="stChatMessage"] p, [data-testid="stChatMessage"] li,
    [data-testid="stChatMessage"] span, [data-testid="stChatMessage"] div { color: #F1F5F9 !important; }
    [data-testid="stChatMessage"] h1, [data-testid="stChatMessage"] h2,
    [data-testid="stChatMessage"] h3, [data-testid="stChatMessage"] h4,
    [data-testid="stChatMessage"] strong { color: #FFFFFF !important; }
    .stChatInput > div { background-color: #1E293B !important; border-color: rgba(148,163,184,0.2) !important; }
    .stChatInput input { color: #F8FAFC !important; }

    /* Caption text */
    .stCaption, [data-testid="stCaptionContainer"] { color: #94A3B8 !important; }

    /* Button text */
    .stButton button { color: #F1F5F9 !important; border-color: rgba(148,163,184,0.2) !important; }

    /* General paragraph text */
    .stMarkdown p, .stMarkdown li, .stMarkdown span { color: #E2E8F0; }

    /* Scrollbar */
    ::-webkit-scrollbar { width: 6px; }
    ::-webkit-scrollbar-track { background: #0F172A; }
    ::-webkit-scrollbar-thumb { background: #334155; border-radius: 3px; }
</style>
""", unsafe_allow_html=True)


# ── Snowflake Connection ──
@st.cache_resource(ttl=3600)
def get_connection():
    import snowflake.connector
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization
    try:
        sf = st.secrets["snowflake"]
        account, user = sf["account"], sf["user"]
        pk_bytes = sf["private_key"].encode("utf-8")
    except Exception:
        account, user = "rjgcbvq-ke67766", "KMASAKI"
        with open(os.path.expanduser("~/.snowflake/keys/snowflake_private_key.p8"), "rb") as f:
            pk_bytes = f.read()
    private_key = serialization.load_pem_private_key(pk_bytes, password=None, backend=default_backend())
    return snowflake.connector.connect(
        account=account, user=user, private_key=private_key,
        role="ACCOUNTADMIN", warehouse="HISAKA_DEMO_WH",
        database="HISAKA_DEMO_DB", schema="SALES",
    )


@st.cache_data(ttl=300)
def run_query(sql):
    conn = get_connection()
    try:
        return pd.read_sql(sql, conn)
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()


def fmt_yen(val):
    if pd.isna(val) or val is None:
        return "—"
    val = float(val)
    if abs(val) >= 100_000_000:
        return f"¥{val / 100_000_000:.1f}億"
    elif abs(val) >= 10_000:
        return f"¥{val / 10_000:,.0f}万"
    return f"¥{val:,.0f}"


def kpi_card(label, value, icon="", color="#3B82F6", sub=""):
    """Glassmorphism-style KPI card."""
    sub_html = f'<div style="font-size:12px;color:#94A3B8;margin-top:4px">{sub}</div>' if sub else ""
    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, {color}22, {color}08);
        border: 1px solid {color}33;
        border-radius: 16px; padding: 24px 20px;
        backdrop-filter: blur(10px);
    ">
        <div style="display:flex;justify-content:space-between;align-items:center">
            <div style="font-size:12px;color:#94A3B8;font-weight:500;text-transform:uppercase;letter-spacing:1px">{label}</div>
            <div style="font-size:20px">{icon}</div>
        </div>
        <div style="font-size:32px;font-weight:800;color:#F8FAFC;margin-top:8px;letter-spacing:-1px">{value}</div>
        {sub_html}
    </div>""", unsafe_allow_html=True)


def apply_layout(fig, **kwargs):
    """Apply consistent Plotly layout."""
    layout = {**PLOTLY_LAYOUT, **kwargs}
    fig.update_layout(**layout)
    return fig


# ── Sidebar ──
with st.sidebar:
    st.markdown("""
    <div style="text-align:center;padding:20px 0">
        <div style="font-size:28px;font-weight:800;background:linear-gradient(135deg,#FF6B35,#FB923C);-webkit-background-clip:text;-webkit-text-fill-color:transparent">HISAKA</div>
        <div style="font-size:13px;color:#94A3B8;margin-top:4px">Salesforce 営業ダッシュボード</div>
    </div>""", unsafe_allow_html=True)
    st.divider()

    fy = st.selectbox("会計年度", [2026, 2025, 2024], index=0)
    fy_start = f"{fy}-04-01"
    fy_end = f"{fy + 1}-03-31"

    seg_filter = st.multiselect("事業部", ["熱交換器事業", "PE事業", "バルブ事業"],
                                 default=["熱交換器事業", "PE事業", "バルブ事業"])
    seg_sql = "','".join(seg_filter)

    st.divider()
    st.markdown("""
    <div style="text-align:center;padding:8px 0">
        <div style="font-size:11px;color:#94A3B8">Powered by</div>
        <div style="font-size:13px;color:#94A3B8;font-weight:600">🔷 Lepont &nbsp;×&nbsp; 🟠 Streamlit</div>
        <div style="font-size:11px;color:#94A3B8;margin-top:4px">thomas株式会社</div>
    </div>""", unsafe_allow_html=True)


# ── Tabs ──
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊 パイプライン概要",
    "📈 売上予測",
    "👤 営業担当分析",
    "🔧 メンテナンス管理",
    "🔗 クロスセル機会",
    "🤖 AI分析",
])


# ================================================================
# TAB 1: Pipeline Overview
# ================================================================
with tab1:
    st.markdown(f"### 📊 営業パイプライン概要")
    st.caption(f"{fy}年度 | {' / '.join(seg_filter)}")

    kpi_sql = f"""
    SELECT
        COUNT(*) AS "total", SUM(CASE WHEN IS_OPEN=1 THEN 1 ELSE 0 END) AS "open",
        SUM(CASE WHEN IS_WON=1 THEN 1 ELSE 0 END) AS "won",
        SUM(CASE WHEN IS_WON=1 THEN AMOUNT ELSE 0 END) AS "won_amt",
        SUM(CASE WHEN IS_OPEN=1 THEN AMOUNT ELSE 0 END) AS "pipe_amt",
        ROUND(SUM(CASE WHEN IS_WON=1 THEN 1 ELSE 0 END) * 100.0 /
              NULLIF(SUM(CASE WHEN IS_WON=1 OR IS_LOST=1 THEN 1 ELSE 0 END), 0), 1) AS "win_rate"
    FROM FACT_OPPORTUNITY o JOIN DIM_SEGMENT s ON o.SEGMENT_ID = s.ID
    WHERE o.DATE_KEY >= '{fy_start}' AND o.DATE_KEY < '{fy_end}' AND s.NAME IN ('{seg_sql}')
    """
    kpi = run_query(kpi_sql)
    if not kpi.empty:
        r = kpi.iloc[0]
        c1, c2, c3, c4, c5 = st.columns(5)
        with c1: kpi_card("パイプライン総額", fmt_yen(r.get("pipe_amt", 0)), "💰", "#3B82F6")
        with c2: kpi_card("受注金額", fmt_yen(r.get("won_amt", 0)), "🏆", "#22C55E")
        with c3: kpi_card("受注率", f"{r.get('win_rate', 0):.1f}%", "📊", "#FF6B35")
        with c4: kpi_card("進行中", f"{int(r.get('open', 0))}件", "🔄", "#06B6D4")
        with c5: kpi_card("全商談", f"{int(r.get('total', 0))}件", "📋", "#64748B")

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    col_l, col_r = st.columns([3, 2])

    with col_l:
        st.markdown("##### ステージ別パイプライン")
        stage_sql = f"""
        SELECT o.STAGE AS "stage", o.STAGE_ID AS "sid",
               COUNT(*) AS "cnt", SUM(o.AMOUNT) AS "amt"
        FROM FACT_OPPORTUNITY o JOIN DIM_SEGMENT s ON o.SEGMENT_ID = s.ID
        WHERE o.IS_OPEN = 1 AND o.DATE_KEY >= '{fy_start}' AND o.DATE_KEY < '{fy_end}'
          AND s.NAME IN ('{seg_sql}')
        GROUP BY o.STAGE, o.STAGE_ID ORDER BY o.STAGE_ID
        """
        df_stage = run_query(stage_sql)
        if not df_stage.empty:
            fig = go.Figure()
            colors = ["#1E3A8A", "#2563EB", "#3B82F6", "#60A5FA", "#93C5FD", "#BFDBFE"]
            for i, row in df_stage.iterrows():
                ci = min(i, len(colors) - 1)
                fig.add_trace(go.Bar(
                    y=[row["stage"]], x=[row["amt"]], orientation="h",
                    marker=dict(color=colors[ci], cornerradius=6),
                    text=f"¥{row['amt']/10000:,.0f}万 ({row['cnt']}件)",
                    textposition="inside", textfont=dict(color="white", size=13),
                    hovertemplate=f"<b>{row['stage']}</b><br>金額: ¥{row['amt']:,.0f}<br>件数: {row['cnt']}件<extra></extra>",
                    showlegend=False,
                ))
            apply_layout(fig, height=350, yaxis=dict(autorange="reversed", showgrid=False),
                         xaxis=dict(showticklabels=False, showgrid=False))
            st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.markdown("##### 事業部別構成")
        seg_sql2 = f"""
        SELECT s.NAME AS "seg", SUM(o.AMOUNT) AS "amt"
        FROM FACT_OPPORTUNITY o JOIN DIM_SEGMENT s ON o.SEGMENT_ID = s.ID
        WHERE o.IS_OPEN = 1 AND o.DATE_KEY >= '{fy_start}' AND o.DATE_KEY < '{fy_end}'
          AND s.NAME IN ('{seg_sql}')
        GROUP BY s.NAME
        """
        df_seg = run_query(seg_sql2)
        if not df_seg.empty:
            fig2 = go.Figure(go.Pie(
                labels=df_seg["seg"], values=df_seg["amt"],
                hole=0.55,
                marker=dict(colors=[SEG_COLORS.get(s, "#666") for s in df_seg["seg"]],
                            line=dict(color="#0F172A", width=3)),
                textinfo="label+percent", textfont=dict(size=13, color="white"),
                hovertemplate="<b>%{label}</b><br>¥%{value:,.0f}<br>%{percent}<extra></extra>",
            ))
            fig2.add_annotation(text=f"<b>{fmt_yen(df_seg['amt'].sum())}</b>",
                                font=dict(size=18, color="#F8FAFC"), showarrow=False)
            apply_layout(fig2, height=350, showlegend=False)
            st.plotly_chart(fig2, use_container_width=True)

    # Stale opportunities
    st.markdown("##### ⚠️ 滞留商談アラート")
    stale_sql = f"""
    SELECT o.OPP_NAME AS "商談名", s.NAME AS "事業部", r.NAME AS "担当者",
           o.STAGE AS "ステージ", o.DAYS_IN_STAGE AS "滞留日数",
           o.AMOUNT AS "金額", o.ACTIVITY_COUNT AS "活動回数"
    FROM FACT_OPPORTUNITY o
    JOIN DIM_SEGMENT s ON o.SEGMENT_ID = s.ID
    JOIN DIM_SALES_REP r ON o.SALES_REP_ID = r.ID
    WHERE o.IS_OPEN = 1 AND o.DAYS_IN_STAGE > 30
      AND o.DATE_KEY >= '{fy_start}' AND o.DATE_KEY < '{fy_end}'
      AND s.NAME IN ('{seg_sql}')
    ORDER BY o.DAYS_IN_STAGE DESC LIMIT 10
    """
    df_stale = run_query(stale_sql)
    if not df_stale.empty:
        st.dataframe(df_stale, use_container_width=True, hide_index=True,
                     column_config={
                         "金額": st.column_config.NumberColumn(format="¥%d"),
                         "滞留日数": st.column_config.ProgressColumn(
                             min_value=0, max_value=200, format="%d日"),
                     })
        st.warning(f"⚠️ {len(df_stale)}件の商談が30日以上滞留中。フォローアップが必要です。")
    else:
        st.success("✅ 滞留商談はありません")


# ================================================================
# TAB 2: Sales Forecast
# ================================================================
with tab2:
    st.markdown("### 📈 売上予測・フォーキャスト")
    st.caption(f"{fy}年度")

    fc_sql = f"""
    SELECT
        SUM(CASE WHEN IS_WON=1 THEN AMOUNT ELSE 0 END) AS "won",
        SUM(CASE WHEN IS_OPEN=1 AND PROBABILITY >= 70 THEN AMOUNT * PROBABILITY / 100 ELSE 0 END) AS "commit",
        SUM(CASE WHEN IS_OPEN=1 AND PROBABILITY >= 40 THEN AMOUNT * PROBABILITY / 100 ELSE 0 END) AS "best",
        SUM(CASE WHEN IS_OPEN=1 THEN AMOUNT * PROBABILITY / 100 ELSE 0 END) AS "weighted"
    FROM FACT_OPPORTUNITY o JOIN DIM_SEGMENT s ON o.SEGMENT_ID = s.ID
    WHERE o.DATE_KEY >= '{fy_start}' AND o.DATE_KEY < '{fy_end}' AND s.NAME IN ('{seg_sql}')
    """
    fc = run_query(fc_sql)
    if not fc.empty:
        r = fc.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        with c1: kpi_card("受注確定", fmt_yen(r.get("won", 0)), "✅", "#22C55E")
        with c2: kpi_card("コミット (≥70%)", fmt_yen(r.get("commit", 0)), "🎯", "#3B82F6")
        with c3: kpi_card("最善ケース (≥40%)", fmt_yen(r.get("best", 0)), "📈", "#F59E0B")
        with c4: kpi_card("加重パイプライン", fmt_yen(r.get("weighted", 0)), "💎", "#8B5CF6")

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    # Forecast waterfall
    st.markdown("##### フォーキャスト構成")
    if not fc.empty:
        r = fc.iloc[0]
        categories = ["受注確定", "コミット加算", "最善ケース加算", "パイプライン加算"]
        vals = [
            float(r.get("won", 0) or 0),
            float(r.get("commit", 0) or 0) - float(r.get("won", 0) or 0),
            float(r.get("best", 0) or 0) - float(r.get("commit", 0) or 0),
            float(r.get("weighted", 0) or 0) - float(r.get("best", 0) or 0),
        ]
        fig_wf = go.Figure(go.Waterfall(
            x=categories, y=vals,
            measure=["absolute", "relative", "relative", "relative"],
            connector=dict(line=dict(color="rgba(148,163,184,0.3)")),
            increasing=dict(marker=dict(color="#3B82F6")),
            decreasing=dict(marker=dict(color="#EF4444")),
            totals=dict(marker=dict(color="#22C55E")),
            texttemplate="%{y:,.0f}", textposition="outside",
            textfont=dict(color="#E2E8F0", size=12),
        ))
        apply_layout(fig_wf, height=380, showlegend=False, yaxis=dict(showticklabels=False, showgrid=False))
        st.plotly_chart(fig_wf, use_container_width=True)

    # Monthly trend
    st.markdown("##### 月別受注推移（前年比較）")
    trend_sql = f"""
    SELECT d.FISCAL_YEAR AS "fy", TO_CHAR(o.DATE_KEY, 'YYYY-MM') AS "month",
           SUM(CASE WHEN o.IS_WON=1 THEN o.AMOUNT ELSE 0 END) AS "amt",
           COUNT(CASE WHEN o.IS_WON=1 THEN 1 END) AS "cnt"
    FROM FACT_OPPORTUNITY o JOIN DIM_DATE d ON o.DATE_KEY = d.DATE_KEY
    JOIN DIM_SEGMENT s ON o.SEGMENT_ID = s.ID
    WHERE d.FISCAL_YEAR IN ({fy}, {fy-1}) AND s.NAME IN ('{seg_sql}')
    GROUP BY d.FISCAL_YEAR, TO_CHAR(o.DATE_KEY, 'YYYY-MM')
    ORDER BY TO_CHAR(o.DATE_KEY, 'YYYY-MM')
    """
    df_trend = run_query(trend_sql)
    if not df_trend.empty:
        df_trend["fy"] = df_trend["fy"].astype(str) + "年度"
        fig = px.bar(df_trend, x="month", y="amt", color="fy",
                     barmode="group", color_discrete_map={
                         f"{fy}年度": "#3B82F6", f"{fy-1}年度": "#334155"})
        fig.update_traces(marker_cornerradius=6, texttemplate="%{y:,.0f}", textposition="outside",
                          textfont=dict(size=10, color="#94A3B8"))
        apply_layout(fig, height=400, xaxis_title="", yaxis_title="",
                     legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        st.plotly_chart(fig, use_container_width=True)

    # Segment forecast table
    st.markdown("##### 事業部別フォーキャスト")
    seg_fc_sql = f"""
    SELECT s.NAME AS "事業部",
           SUM(CASE WHEN o.IS_WON=1 THEN o.AMOUNT ELSE 0 END) AS "受注確定",
           SUM(CASE WHEN o.IS_OPEN=1 THEN o.AMOUNT * o.PROBABILITY / 100 ELSE 0 END) AS "加重PL",
           SUM(CASE WHEN o.IS_WON=1 THEN o.AMOUNT ELSE 0 END) +
           SUM(CASE WHEN o.IS_OPEN=1 THEN o.AMOUNT * o.PROBABILITY / 100 ELSE 0 END) AS "着地予測",
           COUNT(CASE WHEN o.IS_OPEN=1 THEN 1 END) AS "進行中",
           ROUND(SUM(CASE WHEN o.IS_WON=1 THEN 1 ELSE 0 END) * 100.0 /
                 NULLIF(SUM(CASE WHEN o.IS_WON=1 OR o.IS_LOST=1 THEN 1 ELSE 0 END), 0), 1) AS "受注率"
    FROM FACT_OPPORTUNITY o JOIN DIM_SEGMENT s ON o.SEGMENT_ID = s.ID
    WHERE o.DATE_KEY >= '{fy_start}' AND o.DATE_KEY < '{fy_end}' AND s.NAME IN ('{seg_sql}')
    GROUP BY s.NAME ORDER BY "着地予測" DESC
    """
    df_seg_fc = run_query(seg_fc_sql)
    if not df_seg_fc.empty:
        st.dataframe(df_seg_fc, use_container_width=True, hide_index=True,
                     column_config={
                         "受注確定": st.column_config.NumberColumn(format="¥%d"),
                         "加重PL": st.column_config.NumberColumn(format="¥%d"),
                         "着地予測": st.column_config.NumberColumn(format="¥%d"),
                         "受注率": st.column_config.ProgressColumn(min_value=0, max_value=100, format="%.1f%%"),
                     })


# ================================================================
# TAB 3: Sales Rep Analysis
# ================================================================
with tab3:
    st.markdown("### 👤 営業担当別パフォーマンス")
    st.caption(f"{fy}年度")

    rep_sql = f"""
    SELECT r.NAME AS "name", r.ROLE AS "role", s.NAME AS "seg",
           COUNT(*) AS "deals", SUM(CASE WHEN o.IS_WON=1 THEN 1 ELSE 0 END) AS "won",
           ROUND(SUM(CASE WHEN o.IS_WON=1 THEN 1 ELSE 0 END) * 100.0 /
                 NULLIF(SUM(CASE WHEN o.IS_WON=1 OR o.IS_LOST=1 THEN 1 ELSE 0 END), 0), 1) AS "win_rate",
           SUM(CASE WHEN o.IS_WON=1 THEN o.AMOUNT ELSE 0 END) AS "won_amt",
           ROUND(AVG(o.ACTIVITY_COUNT), 1) AS "avg_activity",
           SUM(CASE WHEN o.IS_OPEN=1 AND o.DAYS_IN_STAGE > 30 THEN 1 ELSE 0 END) AS "stale",
           SUM(CASE WHEN o.IS_OPEN=1 THEN o.AMOUNT ELSE 0 END) AS "pipeline"
    FROM FACT_OPPORTUNITY o JOIN DIM_SALES_REP r ON o.SALES_REP_ID = r.ID
    JOIN DIM_SEGMENT s ON o.SEGMENT_ID = s.ID
    WHERE o.DATE_KEY >= '{fy_start}' AND o.DATE_KEY < '{fy_end}' AND s.NAME IN ('{seg_sql}')
    GROUP BY r.NAME, r.ROLE, s.NAME ORDER BY "won_amt" DESC
    """
    df_rep = run_query(rep_sql)
    if not df_rep.empty:
        # Scatter: Activity vs Win Rate (bubble = won amount)
        st.markdown("##### 活動量 × 受注率マトリクス")
        fig = px.scatter(df_rep, x="avg_activity", y="win_rate",
                         size="won_amt", color="seg", color_discrete_map=SEG_COLORS,
                         text="name", size_max=50,
                         hover_data={"deals": True, "stale": True, "name": False})
        fig.update_traces(textposition="top center", textfont=dict(size=12, color="#F8FAFC"))
        # Quadrant lines
        avg_act = df_rep["avg_activity"].mean()
        avg_wr = df_rep["win_rate"].mean()
        fig.add_hline(y=avg_wr, line_dash="dot", line_color="rgba(148,163,184,0.3)",
                      annotation_text=f"平均受注率 {avg_wr:.0f}%", annotation_font_color="#94A3B8")
        fig.add_vline(x=avg_act, line_dash="dot", line_color="rgba(148,163,184,0.3)",
                      annotation_text=f"平均活動 {avg_act:.1f}回", annotation_font_color="#94A3B8")
        apply_layout(fig, height=480, xaxis_title="平均活動回数", yaxis_title="受注率（%）",
                     legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        st.plotly_chart(fig, use_container_width=True)

        # Rep table
        st.markdown("##### 担当者別詳細")
        display_rep = df_rep.rename(columns={
            "name": "担当者", "role": "役職", "seg": "事業部",
            "deals": "商談数", "won": "受注数", "win_rate": "受注率",
            "won_amt": "受注金額", "avg_activity": "活動回数", "stale": "滞留", "pipeline": "パイプライン"
        })
        st.dataframe(display_rep, use_container_width=True, hide_index=True,
                     column_config={
                         "受注金額": st.column_config.NumberColumn(format="¥%d"),
                         "パイプライン": st.column_config.NumberColumn(format="¥%d"),
                         "受注率": st.column_config.ProgressColumn(min_value=0, max_value=100, format="%.1f%%"),
                         "活動回数": st.column_config.ProgressColumn(min_value=0, max_value=15, format="%.1f"),
                     })

        # Alerts
        low_act = df_rep[df_rep["avg_activity"] < 3]
        if not low_act.empty:
            st.warning(f"⚠️ 活動不足: {', '.join(low_act['name'])} — コーチングが必要")
        high_stale = df_rep[df_rep["stale"] > 2]
        if not high_stale.empty:
            st.error(f"🚨 滞留多: {', '.join(high_stale['name'])} — マネージャー介入を推奨")


# ================================================================
# TAB 4: Service/Maintenance
# ================================================================
with tab4:
    st.markdown("### 🔧 メンテナンス・サービス管理")
    st.caption(f"{fy}年度")

    svc_sql = f"""
    SELECT COUNT(*) AS "total",
        SUM(CASE WHEN STATUS IN ('新規','対応中') THEN 1 ELSE 0 END) AS "active",
        SUM(REVENUE) AS "rev", SUM(GROSS_PROFIT) AS "gp",
        ROUND(AVG(CASE WHEN STATUS='完了' THEN SLA_MET END) * 100, 1) AS "sla",
        ROUND(AVG(CASE WHEN STATUS='完了' THEN SATISFACTION_SCORE END), 1) AS "csat"
    FROM FACT_SERVICE_CASE c JOIN DIM_SEGMENT s ON c.SEGMENT_ID = s.ID
    WHERE c.DATE_KEY >= '{fy_start}' AND c.DATE_KEY < '{fy_end}' AND s.NAME IN ('{seg_sql}')
    """
    svc = run_query(svc_sql)
    if not svc.empty:
        r = svc.iloc[0]
        c1, c2, c3, c4, c5 = st.columns(5)
        with c1: kpi_card("メンテ売上", fmt_yen(r.get("rev", 0)), "🔧", "#3B82F6")
        with c2: kpi_card("メンテ粗利", fmt_yen(r.get("gp", 0)), "💰", "#22C55E")
        with c3: kpi_card("全案件", f"{int(r.get('total', 0) or 0)}件", "📋", "#64748B")
        with c4:
            sla_val = float(r.get("sla", 0) or 0)
            kpi_card("SLA達成率", f"{sla_val:.0f}%", "⏱️", "#22C55E" if sla_val >= 80 else "#EF4444")
        with c5:
            csat_val = float(r.get("csat", 0) or 0)
            kpi_card("顧客満足度", f"{csat_val:.1f}/5", "⭐", "#F59E0B")

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    col_l, col_r = st.columns(2)

    with col_l:
        st.markdown("##### ケースタイプ別")
        type_sql = f"""
        SELECT c.CASE_TYPE AS "type", COUNT(*) AS "cnt", SUM(c.REVENUE) AS "rev"
        FROM FACT_SERVICE_CASE c JOIN DIM_SEGMENT s ON c.SEGMENT_ID = s.ID
        WHERE c.DATE_KEY >= '{fy_start}' AND c.DATE_KEY < '{fy_end}' AND s.NAME IN ('{seg_sql}')
        GROUP BY c.CASE_TYPE ORDER BY "rev" DESC
        """
        df_type = run_query(type_sql)
        if not df_type.empty:
            type_colors = ["#3B82F6", "#06B6D4", "#22C55E", "#F59E0B", "#EF4444"]
            fig = go.Figure()
            for i, row in df_type.iterrows():
                fig.add_trace(go.Bar(
                    x=[row["type"]], y=[row["rev"]],
                    marker=dict(color=type_colors[i % len(type_colors)], cornerradius=8),
                    text=f"{row['cnt']}件<br>¥{row['rev']/10000:,.0f}万",
                    textposition="outside", textfont=dict(size=11, color="#94A3B8"),
                    showlegend=False,
                    hovertemplate=f"<b>{row['type']}</b><br>件数: {row['cnt']}<br>売上: ¥{row['rev']:,.0f}<extra></extra>",
                ))
            apply_layout(fig, height=380, xaxis=dict(showgrid=False), yaxis=dict(showticklabels=False, showgrid=False))
            st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.markdown("##### 対応中の案件")
        open_sql = f"""
        SELECT c.CASE_ID AS "ID", a.NAME AS "顧客", p.NAME AS "製品",
               c.CASE_TYPE AS "タイプ", c.PRIORITY AS "優先度"
        FROM FACT_SERVICE_CASE c JOIN DIM_ACCOUNT a ON c.ACCOUNT_ID = a.ID
        JOIN DIM_PRODUCT p ON c.PRODUCT_ID = p.ID JOIN DIM_SEGMENT s ON c.SEGMENT_ID = s.ID
        WHERE c.STATUS IN ('新規', '対応中')
          AND c.DATE_KEY >= '{fy_start}' AND c.DATE_KEY < '{fy_end}' AND s.NAME IN ('{seg_sql}')
        ORDER BY CASE WHEN c.PRIORITY='高' THEN 1 WHEN c.PRIORITY='中' THEN 2 ELSE 3 END LIMIT 10
        """
        df_open = run_query(open_sql)
        if not df_open.empty:
            st.dataframe(df_open, use_container_width=True, hide_index=True)
        else:
            st.success("✅ 対応中の案件はありません")

    # Service to Sales
    st.markdown("##### 💡 メンテナンスからの営業機会")
    s2s_sql = f"""
    SELECT a.NAME AS "顧客", s.NAME AS "事業部", COUNT(c.CASE_ID) AS "メンテ件数",
           SUM(c.REVENUE) AS "メンテ売上", MAX(c.DATE_KEY) AS "最終メンテ日"
    FROM FACT_SERVICE_CASE c JOIN DIM_ACCOUNT a ON c.ACCOUNT_ID = a.ID
    JOIN DIM_SEGMENT s ON c.SEGMENT_ID = s.ID
    WHERE c.STATUS = '完了' AND s.NAME IN ('{seg_sql}')
    GROUP BY a.NAME, s.NAME HAVING COUNT(c.CASE_ID) >= 3
    ORDER BY "メンテ売上" DESC LIMIT 10
    """
    df_s2s = run_query(s2s_sql)
    if not df_s2s.empty:
        st.dataframe(df_s2s, use_container_width=True, hide_index=True,
                     column_config={"メンテ売上": st.column_config.NumberColumn(format="¥%d")})


# ================================================================
# TAB 5: Cross-Sell
# ================================================================
with tab5:
    st.markdown("### 🔗 クロスセル機会の発見")
    st.info("💡 複数事業部と取引可能な顧客から、未開拓の事業部を検出しています")

    cross_sql = f"""
    SELECT a.NAME AS "顧客", a.INDUSTRY AS "業種", a.REGION AS "地域",
           a.SEGMENT_IDS AS "対象事業部",
           LISTAGG(DISTINCT s.NAME, ' / ') WITHIN GROUP (ORDER BY s.NAME) AS "取引実績",
           COUNT(DISTINCT o.OPP_ID) AS "商談数",
           SUM(CASE WHEN o.IS_WON=1 THEN o.AMOUNT ELSE 0 END) AS "受注金額"
    FROM DIM_ACCOUNT a JOIN FACT_OPPORTUNITY o ON a.ID = o.ACCOUNT_ID
    JOIN DIM_SEGMENT s ON o.SEGMENT_ID = s.ID
    WHERE a.SEGMENT_IDS LIKE '%|%'
    GROUP BY a.NAME, a.INDUSTRY, a.REGION, a.SEGMENT_IDS
    ORDER BY "受注金額" DESC
    """
    df_cross = run_query(cross_sql)
    if not df_cross.empty:
        st.dataframe(df_cross, use_container_width=True, hide_index=True,
                     column_config={
                         "受注金額": st.column_config.NumberColumn(format="¥%d"),
                     })
        st.markdown(f"""
        <div style="background:linear-gradient(135deg,#3B82F622,#3B82F608);border:1px solid #3B82F633;
                    border-radius:12px;padding:20px;margin-top:12px">
            <div style="font-size:15px;font-weight:600;color:#F8FAFC">🎯 クロスセル推奨アクション</div>
            <div style="font-size:13px;color:#94A3B8;margin-top:8px">
                <b>{len(df_cross)}社</b>で複数事業部のクロスセル機会があります。<br>
                熱交換器の更新案件訪問時に、バルブやPE装置のニーズもヒアリングしましょう。
            </div>
        </div>""", unsafe_allow_html=True)


# ================================================================
# TAB 6: AI Analysis
# ================================================================
with tab6:
    st.markdown("### 🤖 AI営業アドバイザー")
    st.caption("Salesforceのデータに基づく自動分析")

    c1, c2, c3 = st.columns(3)
    with c1:
        q1 = st.button("📉 滞留商談の原因分析", use_container_width=True)
    with c2:
        q2 = st.button("💰 受注率改善の提案", use_container_width=True)
    with c3:
        q3 = st.button("🔗 クロスセル戦略", use_container_width=True)

    question = None
    if q1: question = "滞留商談が多い原因を分析し、具体的な改善アクションを提案してください"
    elif q2: question = "受注率を改善するために、営業チームが明日からできる具体的なアクションを3つ提案してください"
    elif q3: question = "クロスセル機会を活かすための具体的な営業戦略を提案してください"

    user_q = st.chat_input("質問を入力（例: PE事業の売上予測を教えて）")
    if user_q: question = user_q

    if question:
        with st.chat_message("user"):
            st.write(question)

        ctx_sql = f"""
        SELECT s.NAME AS "seg", COUNT(*) AS "total",
               SUM(CASE WHEN o.IS_WON=1 THEN 1 ELSE 0 END) AS "won",
               SUM(CASE WHEN o.IS_OPEN=1 AND o.DAYS_IN_STAGE > 30 THEN 1 ELSE 0 END) AS "stale",
               ROUND(AVG(o.ACTIVITY_COUNT), 1) AS "avg_act",
               SUM(CASE WHEN o.IS_WON=1 THEN o.AMOUNT ELSE 0 END) AS "won_amt"
        FROM FACT_OPPORTUNITY o JOIN DIM_SEGMENT s ON o.SEGMENT_ID = s.ID
        WHERE o.DATE_KEY >= '{fy_start}' GROUP BY s.NAME
        """
        df_ctx = run_query(ctx_sql)
        context = df_ctx.to_string() if not df_ctx.empty else "データなし"

        system_prompt = """あなたは日阪製作所のSalesforce活用アドバイザーです。
製造業の営業プロセスに精通し、具体的で実行可能なアドバイスを提供します。
日阪製作所は3事業（熱交換器・PE・バルブ）を持つ製造業です。
回答は200字以内で、「誰が → 何を → 期待効果」の形式で提案してください。"""

        try:
            import anthropic
            api_key = st.secrets.get("ANTHROPIC_API_KEY", os.getenv("ANTHROPIC_API_KEY"))
            if not api_key: raise ValueError("no key")
            client = anthropic.Anthropic(api_key=api_key)
            with st.chat_message("assistant"):
                with st.spinner("分析中..."):
                    resp = client.messages.create(
                        model="claude-haiku-4-5-20251001", max_tokens=500,
                        system=system_prompt,
                        messages=[{"role": "user", "content": f"データ:\n{context}\n\n質問: {question}"}],
                    )
                    st.write(resp.content[0].text)
        except Exception:
            with st.chat_message("assistant"):
                try:
                    ctx_esc = context.replace("'", "''")[:2000]
                    q_esc = question.replace("'", "''")
                    cortex_sql = f"""SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-sonnet-4-5',
                        '{system_prompt}\\n\\nデータ:\\n{ctx_esc}\\n\\n質問: {q_esc}') AS "response" """
                    with st.spinner("Cortex AI で分析中..."):
                        df_ai = run_query(cortex_sql)
                    if not df_ai.empty and df_ai.iloc[0]["response"]:
                        st.write(df_ai.iloc[0]["response"])
                    else:
                        st.write("AI分析結果を取得できませんでした。")
                except Exception:
                    st.write("AI分析は現在利用できません。ダッシュボードのデータから直接分析してください。")


# ── Footer ──
st.markdown("""
<div style="text-align:center;padding:24px 0;margin-top:40px;border-top:1px solid rgba(148,163,184,0.1)">
    <span style="font-size:12px;color:#CBD5E1">🏭 日阪製作所 × Salesforce 営業ダッシュボード</span>
    <span style="font-size:12px;color:#94A3B8;margin:0 8px">|</span>
    <span style="font-size:12px;color:#CBD5E1">Powered by 🔷 Lepont + 🟠 Streamlit</span>
    <span style="font-size:12px;color:#94A3B8;margin:0 8px">|</span>
    <span style="font-size:12px;color:#CBD5E1">thomas株式会社</span>
</div>""", unsafe_allow_html=True)
