-- Hisaka Demo: Analytics views with Japanese column names

-- V_PIPELINE_SUMMARY: Pipeline summary by segment and stage
CREATE OR REPLACE VIEW V_PIPELINE_SUMMARY AS
SELECT
    s.NAME AS "事業部名",
    o.STAGE AS "ステージ",
    COUNT(*) AS "商談件数",
    SUM(o.AMOUNT) AS "合計金額",
    AVG(o.AMOUNT) AS "平均金額",
    AVG(o.PROBABILITY) AS "平均確度"
FROM FACT_OPPORTUNITY o
JOIN DIM_SEGMENT s ON o.SEGMENT_ID = s.ID
WHERE o.IS_OPEN = 1
GROUP BY s.NAME, o.STAGE;

-- V_SALES_FORECAST: Monthly sales forecast (weighted pipeline)
CREATE OR REPLACE VIEW V_SALES_FORECAST AS
SELECT
    d.FISCAL_YEAR AS "会計年度",
    d.FISCAL_QUARTER AS "四半期",
    d.YEAR AS "年",
    d.MONTH AS "月",
    s.NAME AS "事業部名",
    COUNT(*) AS "商談件数",
    SUM(o.AMOUNT) AS "パイプライン金額",
    SUM(o.AMOUNT * o.PROBABILITY / 100) AS "加重パイプライン",
    SUM(CASE WHEN o.IS_WON = 1 THEN o.AMOUNT ELSE 0 END) AS "受注金額"
FROM FACT_OPPORTUNITY o
JOIN DIM_SEGMENT s ON o.SEGMENT_ID = s.ID
JOIN DIM_DATE d ON o.DATE_KEY = d.DATE_KEY
GROUP BY d.FISCAL_YEAR, d.FISCAL_QUARTER, d.YEAR, d.MONTH, s.NAME;

-- V_REP_PERFORMANCE: Sales rep performance metrics
CREATE OR REPLACE VIEW V_REP_PERFORMANCE AS
SELECT
    r.NAME AS "担当者名",
    r.ROLE AS "役職",
    s.NAME AS "事業部名",
    r.YEARS AS "経験年数",
    COUNT(*) AS "商談数",
    SUM(CASE WHEN o.IS_WON = 1 THEN 1 ELSE 0 END) AS "受注数",
    ROUND(SUM(CASE WHEN o.IS_WON = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS "受注率",
    SUM(CASE WHEN o.IS_WON = 1 THEN o.AMOUNT ELSE 0 END) AS "受注金額",
    AVG(o.AMOUNT) AS "平均商談金額",
    AVG(CASE WHEN o.IS_WON = 1 OR o.IS_LOST = 1 THEN DATEDIFF('day', o.CREATED_DATE, o.CLOSE_DATE) END) AS "平均商談期間日数",
    SUM(o.ACTIVITY_COUNT) AS "活動数合計",
    AVG(o.ACTIVITY_COUNT) AS "平均活動数"
FROM FACT_OPPORTUNITY o
JOIN DIM_SALES_REP r ON o.SALES_REP_ID = r.ID
JOIN DIM_SEGMENT s ON o.SEGMENT_ID = s.ID
GROUP BY r.NAME, r.ROLE, s.NAME, r.YEARS;

-- V_ACCOUNT_ANALYSIS: Account-level integrated view (sales + service)
CREATE OR REPLACE VIEW V_ACCOUNT_ANALYSIS AS
SELECT
    a.NAME AS "得意先名",
    a.INDUSTRY AS "業界",
    a.REGION AS "地域",
    a.SIZE AS "規模",
    opp."商談数",
    opp."受注数",
    opp."受注金額",
    opp."オープン商談数",
    opp."オープン金額",
    svc."メンテナンス件数",
    svc."メンテナンス売上",
    svc."平均満足度",
    svc."SLA達成率"
FROM DIM_ACCOUNT a
LEFT JOIN (
    SELECT
        ACCOUNT_ID,
        COUNT(*) AS "商談数",
        SUM(IS_WON) AS "受注数",
        SUM(CASE WHEN IS_WON = 1 THEN AMOUNT ELSE 0 END) AS "受注金額",
        SUM(IS_OPEN) AS "オープン商談数",
        SUM(CASE WHEN IS_OPEN = 1 THEN AMOUNT ELSE 0 END) AS "オープン金額"
    FROM FACT_OPPORTUNITY
    GROUP BY ACCOUNT_ID
) opp ON a.ID = opp.ACCOUNT_ID
LEFT JOIN (
    SELECT
        ACCOUNT_ID,
        COUNT(*) AS "メンテナンス件数",
        SUM(REVENUE) AS "メンテナンス売上",
        ROUND(AVG(SATISFACTION_SCORE), 1) AS "平均満足度",
        ROUND(SUM(SLA_MET) * 100.0 / NULLIF(COUNT(*), 0), 1) AS "SLA達成率"
    FROM FACT_SERVICE_CASE
    GROUP BY ACCOUNT_ID
) svc ON a.ID = svc.ACCOUNT_ID;

-- V_CROSS_SELL: Cross-sell opportunities (accounts with multi-segment potential)
CREATE OR REPLACE VIEW V_CROSS_SELL AS
SELECT
    a.NAME AS "得意先名",
    a.INDUSTRY AS "業界",
    a.SIZE AS "規模",
    a.SEGMENT_IDS AS "取引事業部",
    seg.NAME AS "未開拓事業部",
    seg.CODE AS "事業部コード"
FROM DIM_ACCOUNT a
CROSS JOIN DIM_SEGMENT seg
WHERE NOT CONTAINS(a.SEGMENT_IDS, seg.ID)
  AND a.SEGMENT_IDS LIKE '%|%';

-- V_STALE_OPPORTUNITIES: Stale opportunities (stuck > 30 days in stage)
CREATE OR REPLACE VIEW V_STALE_OPPORTUNITIES AS
SELECT
    o.OPP_ID AS "商談ID",
    o.OPP_NAME AS "商談名",
    a.NAME AS "得意先名",
    s.NAME AS "事業部名",
    r.NAME AS "担当者名",
    o.STAGE AS "ステージ",
    o.DAYS_IN_STAGE AS "ステージ滞留日数",
    o.AMOUNT AS "金額",
    o.PROBABILITY AS "確度",
    o.EXPECTED_CLOSE_DATE AS "見込受注日",
    o.CREATED_DATE AS "作成日"
FROM FACT_OPPORTUNITY o
JOIN DIM_ACCOUNT a ON o.ACCOUNT_ID = a.ID
JOIN DIM_SEGMENT s ON o.SEGMENT_ID = s.ID
JOIN DIM_SALES_REP r ON o.SALES_REP_ID = r.ID
WHERE o.IS_OPEN = 1
  AND o.DAYS_IN_STAGE > 30;

-- V_SERVICE_SUMMARY: Service case summary by type
CREATE OR REPLACE VIEW V_SERVICE_SUMMARY AS
SELECT
    s.NAME AS "事業部名",
    sc.CASE_TYPE AS "案件タイプ",
    sc.PRIORITY AS "優先度",
    COUNT(*) AS "件数",
    SUM(sc.REVENUE) AS "売上合計",
    SUM(sc.COST) AS "コスト合計",
    SUM(sc.GROSS_PROFIT) AS "粗利合計",
    ROUND(AVG(sc.RESOLUTION_HOURS), 1) AS "平均解決時間h",
    ROUND(SUM(sc.SLA_MET) * 100.0 / NULLIF(COUNT(*), 0), 1) AS "SLA達成率",
    ROUND(AVG(sc.SATISFACTION_SCORE), 2) AS "平均満足度"
FROM FACT_SERVICE_CASE sc
JOIN DIM_SEGMENT s ON sc.SEGMENT_ID = s.ID
GROUP BY s.NAME, sc.CASE_TYPE, sc.PRIORITY;

-- V_SERVICE_TO_SALES: Service-to-sales conversion potential
CREATE OR REPLACE VIEW V_SERVICE_TO_SALES AS
SELECT
    a.NAME AS "得意先名",
    a.INDUSTRY AS "業界",
    s.NAME AS "事業部名",
    sc."メンテナンス件数",
    sc."直近メンテナンス日",
    sc."平均満足度",
    CASE
        WHEN opp.ACCOUNT_ID IS NULL THEN 'オープン商談なし - 要アプローチ'
        ELSE 'オープン商談あり'
    END AS "営業ステータス"
FROM (
    SELECT
        ACCOUNT_ID,
        SEGMENT_ID,
        COUNT(*) AS "メンテナンス件数",
        MAX(DATE_KEY) AS "直近メンテナンス日",
        ROUND(AVG(SATISFACTION_SCORE), 1) AS "平均満足度"
    FROM FACT_SERVICE_CASE
    GROUP BY ACCOUNT_ID, SEGMENT_ID
) sc
JOIN DIM_ACCOUNT a ON sc.ACCOUNT_ID = a.ID
JOIN DIM_SEGMENT s ON sc.SEGMENT_ID = s.ID
LEFT JOIN (
    SELECT DISTINCT ACCOUNT_ID
    FROM FACT_OPPORTUNITY
    WHERE IS_OPEN = 1
) opp ON sc.ACCOUNT_ID = opp.ACCOUNT_ID;

-- V_MONTHLY_TREND: Monthly won amount and count trend
CREATE OR REPLACE VIEW V_MONTHLY_TREND AS
SELECT
    d.FISCAL_YEAR_LABEL AS "会計年度",
    d.FISCAL_QUARTER AS "四半期",
    d.YEAR AS "年",
    d.MONTH AS "月",
    COUNT(CASE WHEN o.IS_WON = 1 THEN 1 END) AS "受注件数",
    SUM(CASE WHEN o.IS_WON = 1 THEN o.AMOUNT ELSE 0 END) AS "受注金額",
    COUNT(CASE WHEN o.IS_LOST = 1 THEN 1 END) AS "失注件数",
    COUNT(CASE WHEN o.IS_OPEN = 1 THEN 1 END) AS "オープン件数",
    SUM(CASE WHEN o.IS_OPEN = 1 THEN o.AMOUNT ELSE 0 END) AS "オープン金額"
FROM FACT_OPPORTUNITY o
JOIN DIM_DATE d ON o.DATE_KEY = d.DATE_KEY
GROUP BY d.FISCAL_YEAR_LABEL, d.FISCAL_QUARTER, d.YEAR, d.MONTH
ORDER BY d.YEAR, d.MONTH;

-- V_SEGMENT_COMPARISON: Segment comparison metrics
CREATE OR REPLACE VIEW V_SEGMENT_COMPARISON AS
SELECT
    s.NAME AS "事業部名",
    s.CODE AS "事業部コード",
    s.REVENUE_RATIO AS "売上構成比",
    opp."商談数",
    opp."受注数",
    opp."受注率",
    opp."平均商談金額",
    opp."受注金額合計",
    svc."メンテナンス件数",
    svc."メンテナンス売上",
    ROUND(svc."メンテナンス件数" * 100.0 / NULLIF(opp."商談数", 0), 1) AS "メンテナンス比率"
FROM DIM_SEGMENT s
LEFT JOIN (
    SELECT
        SEGMENT_ID,
        COUNT(*) AS "商談数",
        SUM(IS_WON) AS "受注数",
        ROUND(SUM(IS_WON) * 100.0 / NULLIF(COUNT(*), 0), 1) AS "受注率",
        ROUND(AVG(AMOUNT)) AS "平均商談金額",
        SUM(CASE WHEN IS_WON = 1 THEN AMOUNT ELSE 0 END) AS "受注金額合計"
    FROM FACT_OPPORTUNITY
    GROUP BY SEGMENT_ID
) opp ON s.ID = opp.SEGMENT_ID
LEFT JOIN (
    SELECT
        SEGMENT_ID,
        COUNT(*) AS "メンテナンス件数",
        SUM(REVENUE) AS "メンテナンス売上"
    FROM FACT_SERVICE_CASE
    GROUP BY SEGMENT_ID
) svc ON s.ID = svc.SEGMENT_ID;
