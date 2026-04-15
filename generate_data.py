"""
日阪製作所 Snowflakeデモ用データ生成（IR実績連動版）
====================================================
RESEARCH_REPORT.md の IR実績データに基づき、セグメント別売上高が
実績と整合するようにパラメトリック生成する。

会計年度対応:
  FY2020 = 2020/4-2021/3 = 2021/03期
  FY2021 = 2021/4-2022/3 = 2022/03期
  ...
  FY2025 = 2025/4-2026/3 = 2026/03期（予想）
  FY2026 = 2026/4-2026/9 = 進行中（H1のみ）
"""

import csv
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

OUTPUT_DIR = Path(__file__).parent / "output" / "csv"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# IR実績データ（単位: 百万円）
# key = 決算期 (例: "2021/03") → FY2020
# ============================================================
IR_DATA = {
    "2016/03": {"total": 25393, "HEX": None, "PE": None, "VLV": None, "op_margin": 4.7},
    "2017/03": {"total": 25023, "HEX": None, "PE": None, "VLV": None, "op_margin": 4.6},
    "2018/03": {"total": 26891, "HEX": None, "PE": None, "VLV": None, "op_margin": 6.0},
    "2019/03": {"total": 30939, "HEX": None, "PE": None, "VLV": None, "op_margin": 6.6},
    "2020/03": {"total": 32511, "HEX": None, "PE": None, "VLV": None, "op_margin": 7.0},
    "2021/03": {"total": 28437, "HEX": 11300, "PE": 13100, "VLV": 3950, "op_margin": 5.0},
    "2022/03": {"total": 30085, "HEX": 11700, "PE": 13900, "VLV": 4450, "op_margin": 6.0},
    "2023/03": {"total": 34074, "HEX": 13400, "PE": 15900, "VLV": 4700, "op_margin": 5.6},
    "2024/03": {"total": 34180, "HEX": 15200, "PE": 14000, "VLV": 4950, "op_margin": 7.2},
    "2025/03": {"total": 38353, "HEX": 16200, "PE": 17200, "VLV": 4950, "op_margin": 7.6},
    "2026/03": {"total": 44000, "HEX": 18700, "PE": 19800, "VLV": 5500, "op_margin": 6.8},  # 予想
    "2027/03": {"total": 48000, "HEX": 20500, "PE": 21500, "VLV": 6000, "op_margin": 7.5},  # FY2026推定（成長率維持）
}

# FY mapping: fiscal_year -> 決算期
def fy_to_period(fy):
    """FY2024 (2024/4-2025/3) -> '2025/03'"""
    return f"{fy+1}/03"

# Segment revenue for FYs with segment data (FY2020-FY2025)
def get_segment_revenue(fy, seg_code):
    """Get segment revenue in 百万円 for a fiscal year."""
    period = fy_to_period(fy)
    ir = IR_DATA.get(period, {})
    val = ir.get(seg_code)
    if val:
        return val
    # For older years without segment data, estimate from total using 2021/03 ratios
    total = ir.get("total", 30000)
    ratios = {"HEX": 0.40, "PE": 0.46, "VLV": 0.14}
    return int(total * ratios[seg_code])


# ============================================================
# Dimension Data
# ============================================================
SEGMENTS = [
    {"id": "SEG001", "name": "熱交換器事業", "code": "HEX"},
    {"id": "SEG002", "name": "PE事業", "code": "PE"},
    {"id": "SEG003", "name": "バルブ事業", "code": "VLV"},
]

PRODUCTS = [
    {"id": "P001", "name": "プレート式熱交換器（標準）", "segment_id": "SEG001", "category": "標準品", "avg_deal": 50, "maint_months": 12},
    {"id": "P002", "name": "プレート式熱交換器（大型）", "segment_id": "SEG001", "category": "大型", "avg_deal": 350, "maint_months": 6},
    {"id": "P003", "name": "CCS用熱交換器 SX-80", "segment_id": "SEG001", "category": "環境対応", "avg_deal": 500, "maint_months": 6},
    {"id": "P004", "name": "ヒートポンプ用熱交換器", "segment_id": "SEG001", "category": "省エネ", "avg_deal": 150, "maint_months": 12},
    {"id": "P005", "name": "船舶用熱交換器", "segment_id": "SEG001", "category": "船舶", "avg_deal": 250, "maint_months": 12},
    {"id": "P101", "name": "レトルト殺菌装置", "segment_id": "SEG002", "category": "食品", "avg_deal": 800, "maint_months": 6},
    {"id": "P102", "name": "医薬滅菌装置", "segment_id": "SEG002", "category": "医薬", "avg_deal": 1200, "maint_months": 3},
    {"id": "P103", "name": "染色仕上機器", "segment_id": "SEG002", "category": "繊維", "avg_deal": 400, "maint_months": 12},
    {"id": "P104", "name": "MVR付濃縮装置", "segment_id": "SEG002", "category": "環境", "avg_deal": 600, "maint_months": 6},
    {"id": "P201", "name": "ボールバルブ", "segment_id": "SEG003", "category": "汎用", "avg_deal": 5, "maint_months": 24},
    {"id": "P202", "name": "ダイヤフラムバルブ", "segment_id": "SEG003", "category": "高純度", "avg_deal": 8, "maint_months": 24},
    {"id": "P203", "name": "サニタリーバルブ", "segment_id": "SEG003", "category": "食品/医薬", "avg_deal": 12, "maint_months": 12},
]

ACCOUNTS = [
    {"id": "A001", "name": "東洋化学工業", "industry": "化学", "region": "関東", "size": "大手", "seg": ["SEG001", "SEG003"]},
    {"id": "A002", "name": "日清食品プラント", "industry": "食品", "region": "関西", "size": "大手", "seg": ["SEG002"]},
    {"id": "A003", "name": "大塚製薬エンジニアリング", "industry": "医薬品", "region": "関西", "size": "大手", "seg": ["SEG002", "SEG003"]},
    {"id": "A004", "name": "サウジアラムコ", "industry": "エネルギー", "region": "海外", "size": "大手", "seg": ["SEG001"]},
    {"id": "A005", "name": "ENEOS", "industry": "石油化学", "region": "関東", "size": "大手", "seg": ["SEG001"]},
    {"id": "A006", "name": "カゴメ", "industry": "食品", "region": "東海", "size": "大手", "seg": ["SEG002"]},
    {"id": "A007", "name": "味の素エンジニアリング", "industry": "食品", "region": "関東", "size": "大手", "seg": ["SEG002", "SEG001"]},
    {"id": "A008", "name": "三菱ケミカル", "industry": "化学", "region": "関東", "size": "大手", "seg": ["SEG001", "SEG003"]},
    {"id": "A009", "name": "旭化成エンジニアリング", "industry": "化学", "region": "関東", "size": "大手", "seg": ["SEG001"]},
    {"id": "A010", "name": "テルモ", "industry": "医療機器", "region": "関東", "size": "大手", "seg": ["SEG002"]},
    {"id": "A011", "name": "JFEエンジニアリング", "industry": "鉄鋼", "region": "関東", "size": "大手", "seg": ["SEG001"]},
    {"id": "A012", "name": "丸紅プラント", "industry": "商社", "region": "関東", "size": "大手", "seg": ["SEG001", "SEG002"]},
    {"id": "A013", "name": "明治乳業", "industry": "食品", "region": "関東", "size": "大手", "seg": ["SEG002"]},
    {"id": "A014", "name": "ダイキン工業", "industry": "空調", "region": "関西", "size": "大手", "seg": ["SEG001"]},
    {"id": "A015", "name": "花王", "industry": "化学", "region": "関東", "size": "大手", "seg": ["SEG002", "SEG003"]},
    {"id": "A016", "name": "クラレ", "industry": "化学", "region": "関西", "size": "中堅", "seg": ["SEG001"]},
    {"id": "A017", "name": "カーボンテック", "industry": "環境", "region": "北海道", "size": "中堅", "seg": ["SEG001"]},
    {"id": "A018", "name": "東レエンジニアリング", "industry": "繊維", "region": "関西", "size": "大手", "seg": ["SEG002"]},
    {"id": "A019", "name": "住友化学", "industry": "化学", "region": "関西", "size": "大手", "seg": ["SEG001", "SEG003"]},
    {"id": "A020", "name": "マレーシアペトロナス", "industry": "エネルギー", "region": "海外", "size": "大手", "seg": ["SEG001"]},
    {"id": "A021", "name": "協和キリン", "industry": "医薬品", "region": "関東", "size": "大手", "seg": ["SEG002"]},
    {"id": "A022", "name": "サントリープロダクツ", "industry": "飲料", "region": "関西", "size": "大手", "seg": ["SEG002"]},
    {"id": "A023", "name": "日本製鉄", "industry": "鉄鋼", "region": "関東", "size": "大手", "seg": ["SEG001"]},
    {"id": "A024", "name": "三菱重工", "industry": "重工業", "region": "関東", "size": "大手", "seg": ["SEG001"]},
    {"id": "A025", "name": "中部電力", "industry": "電力", "region": "東海", "size": "大手", "seg": ["SEG001"]},
]

SALES_REPS = [
    {"id": "SR01", "name": "山田太郎", "segment_id": "SEG001", "years": 15, "role": "マネージャー"},
    {"id": "SR02", "name": "田中次郎", "segment_id": "SEG001", "years": 8, "role": "担当"},
    {"id": "SR03", "name": "佐藤花子", "segment_id": "SEG001", "years": 3, "role": "担当"},
    {"id": "SR04", "name": "鈴木一郎", "segment_id": "SEG001", "years": 12, "role": "担当"},
    {"id": "SR05", "name": "高橋美咲", "segment_id": "SEG002", "years": 10, "role": "マネージャー"},
    {"id": "SR06", "name": "渡辺健", "segment_id": "SEG002", "years": 6, "role": "担当"},
    {"id": "SR07", "name": "伊藤直樹", "segment_id": "SEG002", "years": 2, "role": "担当"},
    {"id": "SR08", "name": "小林誠", "segment_id": "SEG003", "years": 14, "role": "マネージャー"},
    {"id": "SR09", "name": "中村裕介", "segment_id": "SEG003", "years": 5, "role": "担当"},
    {"id": "SR10", "name": "松本新人", "segment_id": "SEG001", "years": 0, "role": "新任"},
]

STAGES = [
    {"id": 1, "name": "初回訪問", "probability": 10, "avg_days": 14},
    {"id": 2, "name": "要件確認", "probability": 25, "avg_days": 21},
    {"id": 3, "name": "提案準備", "probability": 40, "avg_days": 14},
    {"id": 4, "name": "提案済み", "probability": 55, "avg_days": 21},
    {"id": 5, "name": "見積提出", "probability": 70, "avg_days": 14},
    {"id": 6, "name": "交渉中", "probability": 85, "avg_days": 28},
    {"id": 7, "name": "受注", "probability": 100, "avg_days": 0},
    {"id": 8, "name": "失注", "probability": 0, "avg_days": 0},
]

COMPETITORS = ["アルファラバル", "GEA", "SPX FLOW", "なし", "なし", "なし", "不明"]


def write_csv(filename, rows, fieldnames):
    path = OUTPUT_DIR / filename
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  {filename}: {len(rows)} rows")


def generate_dates():
    rows = []
    d = datetime(2015, 4, 1)
    end = datetime(2026, 9, 30)
    while d <= end:
        fy = d.year if d.month >= 4 else d.year - 1
        q = (d.month - 4) % 12 // 3 + 1 if d.month >= 4 else (d.month + 8) // 3
        rows.append({
            "DATE_KEY": d.strftime("%Y-%m-%d"),
            "YEAR": d.year, "MONTH": d.month, "DAY": d.day,
            "WEEKDAY": ["月", "火", "水", "木", "金", "土", "日"][d.weekday()],
            "IS_WEEKDAY": 1 if d.weekday() < 5 else 0,
            "FISCAL_YEAR": fy,
            "FISCAL_QUARTER": f"Q{q}",
            "FISCAL_YEAR_LABEL": f"{fy}年度",
        })
        d += timedelta(days=1)
    return rows


def generate_opportunities():
    """Generate opportunities so that won amounts match IR segment revenue."""
    rows = []
    opp_id = 1000
    now = datetime(2026, 9, 30)

    # Generate per fiscal year (FY2015 to FY2026)
    for fy in range(2015, 2027):
        period = fy_to_period(fy)
        ir = IR_DATA.get(period)
        if not ir:
            continue

        fy_start = datetime(fy, 4, 1)
        fy_end = datetime(fy + 1, 3, 31)
        if fy == 2026:
            fy_end = datetime(2026, 9, 30)  # H1 only

        for seg in SEGMENTS:
            seg_rev = get_segment_revenue(fy, seg["code"])  # 百万円
            if fy == 2026:
                seg_rev = int(seg_rev * 0.45)  # H1 = ~45% of full year

            # Determine won amount target
            # Won deals should sum to ~seg_rev (in 百万円)
            # Strategy: generate pipeline 2.5x, win rate ~40%
            target_won = seg_rev  # 百万円
            pipeline_target = int(target_won * 2.5)

            seg_products = [p for p in PRODUCTS if p["segment_id"] == seg["id"]]
            seg_reps = [r for r in SALES_REPS if r["segment_id"] == seg["id"]]
            seg_accounts = [a for a in ACCOUNTS if seg["id"] in a["seg"]]

            # Calculate number of deals based on average deal size
            avg_deal_size = sum(p["avg_deal"] for p in seg_products) / len(seg_products)  # 百万円
            n_deals = max(10, int(pipeline_target / avg_deal_size))
            # FY2026 is H1 only but needs a healthy pipeline for demo
            if fy == 2026:
                n_deals = max(n_deals, int(n_deals * 1.2))

            # Track won total to match IR
            won_total = 0
            won_target_remaining = target_won

            for deal_i in range(n_deals):
                opp_id += 1
                product = random.choice(seg_products)
                rep = random.choice(seg_reps)
                account = random.choice(seg_accounts)

                # Deal amount in 百万円, convert to 円 for storage
                deal_mm = max(1, int(product["avg_deal"] * random.uniform(0.3, 2.5)))
                amount = deal_mm * 1_000_000  # 円

                # Create date within fiscal year
                days_in_fy = (fy_end - fy_start).days
                create_date = fy_start + timedelta(days=random.randint(0, max(0, days_in_fy - 1)))

                # Determine outcome
                months_elapsed = (now - create_date).days / 30

                if fy <= 2025:
                    # Historical years: all deals should be closed
                    if won_target_remaining > 0 and random.random() < 0.48:
                        final_stage = 7  # Won
                        won_total += deal_mm
                        won_target_remaining -= deal_mm
                        close_date = create_date + timedelta(days=random.randint(30, 180))
                    else:
                        final_stage = 8  # Lost
                        close_date = create_date + timedelta(days=random.randint(14, 120))
                elif fy == 2026:
                    # Current year (H1 in progress): many open deals for demo
                    if months_elapsed > 4:
                        # Older deals in H1 - some closed
                        r_val = random.random()
                        if r_val < 0.25:
                            final_stage = 7  # Won
                            won_total += deal_mm
                            close_date = create_date + timedelta(days=random.randint(30, 90))
                        elif r_val < 0.40:
                            final_stage = 8  # Lost
                            close_date = create_date + timedelta(days=random.randint(14, 60))
                        else:
                            final_stage = random.randint(3, 6)  # In pipeline
                            close_date = None
                    elif months_elapsed > 2:
                        # Mid-H1 deals
                        r_val = random.random()
                        if r_val < 0.15:
                            final_stage = 7
                            won_total += deal_mm
                            close_date = create_date + timedelta(days=random.randint(20, 60))
                        elif r_val < 0.25:
                            final_stage = 8
                            close_date = create_date + timedelta(days=random.randint(14, 45))
                        else:
                            final_stage = random.randint(2, 5)
                            close_date = None
                    else:
                        # Recent - early stage pipeline
                        final_stage = random.randint(1, 3)
                        close_date = None

                stage_name = STAGES[final_stage - 1]["name"]
                probability = STAGES[final_stage - 1]["probability"]

                # Days in stage
                if final_stage in (7, 8):
                    days_in_stage = 0
                elif fy < 2026 and final_stage < 7:
                    days_in_stage = random.randint(60, 200)  # stale (shouldn't happen for closed years)
                else:
                    days_in_stage = random.randint(3, STAGES[final_stage - 1]["avg_days"] * 2)

                expected_close = close_date if close_date else create_date + timedelta(days=random.randint(60, 180))
                competitor = random.choice(COMPETITORS)

                # New rep (SR10) - lower performance, only in FY2025+
                if rep["id"] == "SR10" and fy >= 2025:
                    if final_stage >= 4:
                        final_stage = random.randint(1, 3)
                        stage_name = STAGES[final_stage - 1]["name"]
                        probability = STAGES[final_stage - 1]["probability"]
                    days_in_stage = int(days_in_stage * 1.5)

                cross_sell = 1 if len(account["seg"]) > 1 else 0

                # Activity count
                if rep["years"] >= 10:
                    activity = random.randint(5, 15)
                elif rep["years"] >= 3:
                    activity = random.randint(3, 10)
                else:
                    activity = random.randint(0, 4)

                rows.append({
                    "OPP_ID": f"OPP-{opp_id}",
                    "DATE_KEY": create_date.strftime("%Y-%m-%d"),
                    "ACCOUNT_ID": account["id"],
                    "SEGMENT_ID": seg["id"],
                    "PRODUCT_ID": product["id"],
                    "SALES_REP_ID": rep["id"],
                    "OPP_NAME": f"{account['name']} {product['name']}",
                    "STAGE": stage_name, "STAGE_ID": final_stage,
                    "AMOUNT": amount, "PROBABILITY": probability,
                    "EXPECTED_CLOSE_DATE": expected_close.strftime("%Y-%m-%d"),
                    "CLOSE_DATE": close_date.strftime("%Y-%m-%d") if close_date else "",
                    "DAYS_IN_STAGE": days_in_stage,
                    "IS_WON": 1 if final_stage == 7 else 0,
                    "IS_LOST": 1 if final_stage == 8 else 0,
                    "IS_OPEN": 1 if final_stage < 7 else 0,
                    "COMPETITOR": competitor,
                    "CROSS_SELL_POTENTIAL": cross_sell,
                    "ACTIVITY_COUNT": activity,
                    "CREATED_DATE": create_date.strftime("%Y-%m-%d"),
                })

            print(f"    FY{fy} {seg['code']}: target={target_won}百万 won={won_total}百万 ({n_deals}件)")

    return rows


def generate_service_cases(opportunities):
    """Generate service cases from won opportunities."""
    rows = []
    case_id = 5000
    won_opps = [o for o in opportunities if o["IS_WON"] == 1 and o["CLOSE_DATE"]]
    end_date = datetime(2026, 9, 30)

    for opp in won_opps:
        product = next((p for p in PRODUCTS if p["id"] == opp["PRODUCT_ID"]), None)
        if not product:
            continue

        install_date = datetime.strptime(opp["CLOSE_DATE"], "%Y-%m-%d")
        interval = product["maint_months"]
        case_date = install_date + timedelta(days=interval * 30)

        while case_date <= end_date:
            case_id += 1
            r = random.random()
            if r < 0.50:
                case_type, priority = "定期メンテナンス", "中"
                hours = random.randint(4, 24)
                revenue = int(opp["AMOUNT"] * random.uniform(0.02, 0.08))
            elif r < 0.70:
                case_type, priority = "部品交換", "中"
                hours = random.randint(8, 48)
                revenue = int(opp["AMOUNT"] * random.uniform(0.03, 0.10))
            elif r < 0.85:
                case_type, priority = "技術問合せ", "低"
                hours, revenue = random.randint(1, 8), 0
            elif r < 0.95:
                case_type, priority = "緊急修理", "高"
                hours = random.randint(2, 72)
                revenue = int(opp["AMOUNT"] * random.uniform(0.05, 0.15))
            else:
                case_type, priority = "改造・増設", "中"
                hours = random.randint(24, 160)
                revenue = int(opp["AMOUNT"] * random.uniform(0.10, 0.30))

            cost = int(revenue * random.uniform(0.3, 0.7)) if revenue > 0 else random.randint(10000, 100000)
            sla_target = 24 if priority == "高" else 72 if priority == "中" else 168
            sla_met = 1 if hours <= sla_target else 0
            sat = random.choice([3, 4, 4, 4, 5, 5]) if sla_met else random.choice([1, 2, 2, 3, 3])
            days_ago = (end_date - case_date).days
            status = random.choice(["新規", "対応中"]) if days_ago < 7 else "完了"

            rows.append({
                "CASE_ID": f"CS-{case_id}", "DATE_KEY": case_date.strftime("%Y-%m-%d"),
                "ACCOUNT_ID": opp["ACCOUNT_ID"], "SEGMENT_ID": opp["SEGMENT_ID"],
                "PRODUCT_ID": opp["PRODUCT_ID"], "RELATED_OPP_ID": opp["OPP_ID"],
                "CASE_TYPE": case_type, "PRIORITY": priority, "STATUS": status,
                "RESOLUTION_HOURS": hours if status == "完了" else None,
                "SLA_TARGET_HOURS": sla_target,
                "SLA_MET": sla_met if status == "完了" else None,
                "REVENUE": revenue, "COST": cost, "GROSS_PROFIT": revenue - cost,
                "SATISFACTION_SCORE": sat if status == "完了" else None,
            })
            case_date += timedelta(days=interval * 30 + random.randint(-15, 30))

    return rows


def verify_ir_alignment(opportunities):
    """Verify that generated data aligns with IR actuals."""
    print("\n=== IR整合性チェック ===")
    print(f"{'FY':>6} {'セグメント':>10} {'IR実績':>10} {'生成受注':>10} {'差異':>8} {'差異率':>8}")
    print("-" * 60)

    for fy in range(2020, 2027):
        for seg in SEGMENTS:
            ir_rev = get_segment_revenue(fy, seg["code"])
            if fy == 2026:
                ir_rev = int(ir_rev * 0.45)

            fy_start = datetime(fy, 4, 1)
            fy_end = datetime(fy + 1, 3, 31) if fy < 2026 else datetime(2026, 9, 30)

            won_amt = sum(
                o["AMOUNT"] for o in opportunities
                if o["IS_WON"] == 1 and o["SEGMENT_ID"] == seg["id"]
                and fy_start <= datetime.strptime(o["DATE_KEY"], "%Y-%m-%d") <= fy_end
            ) / 1_000_000  # 百万円

            diff = won_amt - ir_rev
            pct = (diff / ir_rev * 100) if ir_rev > 0 else 0
            flag = "✓" if abs(pct) < 30 else "△" if abs(pct) < 50 else "✗"
            print(f"FY{fy:>4} {seg['code']:>10} {ir_rev:>8}M {won_amt:>8.0f}M {diff:>+7.0f}M {pct:>+6.1f}% {flag}")


def main():
    print("=== 日阪製作所 デモデータ生成（IR実績連動版）===\n")

    # DIM tables
    print("Dimension tables:")
    write_csv("dim_segment.csv",
              [{"id": s["id"], "name": s["name"], "code": s["code"], "revenue_ratio": 0} for s in SEGMENTS],
              ["id", "name", "code", "revenue_ratio"])

    write_csv("dim_product.csv",
              [{"id": p["id"], "name": p["name"], "segment_id": p["segment_id"],
                "category": p["category"], "avg_price": p["avg_deal"] * 1_000_000,
                "maint_interval_months": p["maint_months"]} for p in PRODUCTS],
              ["id", "name", "segment_id", "category", "avg_price", "maint_interval_months"])

    write_csv("dim_account.csv",
              [{"id": a["id"], "name": a["name"], "industry": a["industry"],
                "region": a["region"], "size": a["size"],
                "segment_ids": "|".join(a["seg"])} for a in ACCOUNTS],
              ["id", "name", "industry", "region", "size", "segment_ids"])

    write_csv("dim_sales_rep.csv",
              [{"id": r["id"], "name": r["name"], "segment_id": r["segment_id"],
                "years": r["years"], "role": r["role"]} for r in SALES_REPS],
              ["id", "name", "segment_id", "years", "role"])

    write_csv("dim_stage.csv",
              [{"id": s["id"], "name": s["name"], "probability": s["probability"],
                "avg_days": s["avg_days"]} for s in STAGES],
              ["id", "name", "probability", "avg_days"])

    dates = generate_dates()
    write_csv("dim_date.csv", dates,
              ["DATE_KEY", "YEAR", "MONTH", "DAY", "WEEKDAY", "IS_WEEKDAY",
               "FISCAL_YEAR", "FISCAL_QUARTER", "FISCAL_YEAR_LABEL"])

    # FACT tables
    print("\nFact tables:")
    print("  Generating opportunities (IR-aligned)...")
    opportunities = generate_opportunities()
    write_csv("fact_opportunity.csv", opportunities,
              ["OPP_ID", "DATE_KEY", "ACCOUNT_ID", "SEGMENT_ID", "PRODUCT_ID",
               "SALES_REP_ID", "OPP_NAME", "STAGE", "STAGE_ID", "AMOUNT",
               "PROBABILITY", "EXPECTED_CLOSE_DATE", "CLOSE_DATE",
               "DAYS_IN_STAGE", "IS_WON", "IS_LOST", "IS_OPEN",
               "COMPETITOR", "CROSS_SELL_POTENTIAL", "ACTIVITY_COUNT", "CREATED_DATE"])

    cases = generate_service_cases(opportunities)
    write_csv("fact_service_case.csv", cases,
              ["CASE_ID", "DATE_KEY", "ACCOUNT_ID", "SEGMENT_ID", "PRODUCT_ID",
               "RELATED_OPP_ID", "CASE_TYPE", "PRIORITY", "STATUS",
               "RESOLUTION_HOURS", "SLA_TARGET_HOURS", "SLA_MET",
               "REVENUE", "COST", "GROSS_PROFIT", "SATISFACTION_SCORE"])

    # Verify
    verify_ir_alignment(opportunities)

    print(f"\n=== 完了 ===")
    print(f"商談数: {len(opportunities)} (受注: {sum(1 for o in opportunities if o['IS_WON'])}, "
          f"失注: {sum(1 for o in opportunities if o['IS_LOST'])}, "
          f"進行中: {sum(1 for o in opportunities if o['IS_OPEN'])})")
    print(f"サービス案件: {len(cases)}")


if __name__ == "__main__":
    main()
