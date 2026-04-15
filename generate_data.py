"""
日阪製作所 Snowflakeデモ用データ生成
====================================
3事業（熱交換器/PE/バルブ）× 商談 + メンテナンス案件
RESEARCH_REPORT.md の実際の財務データに基づくパラメトリック生成

出力: output/csv/ に各テーブルCSV
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
# Dimension Data
# ============================================================

SEGMENTS = [
    {"id": "SEG001", "name": "熱交換器事業", "code": "HEX", "revenue_ratio": 0.42},
    {"id": "SEG002", "name": "PE事業", "code": "PE", "revenue_ratio": 0.45},
    {"id": "SEG003", "name": "バルブ事業", "code": "VLV", "revenue_ratio": 0.13},
]

PRODUCTS = [
    # Heat Exchanger
    {"id": "P001", "name": "プレート式熱交換器（標準）", "segment_id": "SEG001", "category": "標準品", "avg_price": 5000000, "maint_interval_months": 12},
    {"id": "P002", "name": "プレート式熱交換器（大型）", "segment_id": "SEG001", "category": "大型", "avg_price": 35000000, "maint_interval_months": 6},
    {"id": "P003", "name": "CCS用熱交換器 SX-80", "segment_id": "SEG001", "category": "環境対応", "avg_price": 50000000, "maint_interval_months": 6},
    {"id": "P004", "name": "ヒートポンプ用熱交換器", "segment_id": "SEG001", "category": "省エネ", "avg_price": 15000000, "maint_interval_months": 12},
    {"id": "P005", "name": "船舶用熱交換器", "segment_id": "SEG001", "category": "船舶", "avg_price": 25000000, "maint_interval_months": 12},
    # PE
    {"id": "P101", "name": "レトルト殺菌装置", "segment_id": "SEG002", "category": "食品", "avg_price": 80000000, "maint_interval_months": 6},
    {"id": "P102", "name": "医薬滅菌装置", "segment_id": "SEG002", "category": "医薬", "avg_price": 120000000, "maint_interval_months": 3},
    {"id": "P103", "name": "染色仕上機器", "segment_id": "SEG002", "category": "繊維", "avg_price": 40000000, "maint_interval_months": 12},
    {"id": "P104", "name": "MVR付濃縮装置", "segment_id": "SEG002", "category": "環境", "avg_price": 60000000, "maint_interval_months": 6},
    # Valve
    {"id": "P201", "name": "ボールバルブ", "segment_id": "SEG003", "category": "汎用", "avg_price": 500000, "maint_interval_months": 24},
    {"id": "P202", "name": "ダイヤフラムバルブ", "segment_id": "SEG003", "category": "高純度", "avg_price": 800000, "maint_interval_months": 24},
    {"id": "P203", "name": "サニタリーバルブ", "segment_id": "SEG003", "category": "食品/医薬", "avg_price": 1200000, "maint_interval_months": 12},
]

ACCOUNTS = [
    {"id": "A001", "name": "東洋化学工業", "industry": "化学", "region": "関東", "size": "大手", "segment_ids": ["SEG001", "SEG003"]},
    {"id": "A002", "name": "日清食品プラント", "industry": "食品", "region": "関西", "size": "大手", "segment_ids": ["SEG002"]},
    {"id": "A003", "name": "大塚製薬エンジニアリング", "industry": "医薬品", "region": "関西", "size": "大手", "segment_ids": ["SEG002", "SEG003"]},
    {"id": "A004", "name": "サウジアラムコ", "industry": "エネルギー", "region": "海外", "size": "大手", "segment_ids": ["SEG001"]},
    {"id": "A005", "name": "ENEOS", "industry": "石油化学", "region": "関東", "size": "大手", "segment_ids": ["SEG001"]},
    {"id": "A006", "name": "カゴメ", "industry": "食品", "region": "東海", "size": "大手", "segment_ids": ["SEG002"]},
    {"id": "A007", "name": "味の素エンジニアリング", "industry": "食品", "region": "関東", "size": "大手", "segment_ids": ["SEG002", "SEG001"]},
    {"id": "A008", "name": "三菱ケミカル", "industry": "化学", "region": "関東", "size": "大手", "segment_ids": ["SEG001", "SEG003"]},
    {"id": "A009", "name": "旭化成エンジニアリング", "industry": "化学", "region": "関東", "size": "大手", "segment_ids": ["SEG001"]},
    {"id": "A010", "name": "テルモ", "industry": "医療機器", "region": "関東", "size": "大手", "segment_ids": ["SEG002"]},
    {"id": "A011", "name": "JFEエンジニアリング", "industry": "鉄鋼", "region": "関東", "size": "大手", "segment_ids": ["SEG001"]},
    {"id": "A012", "name": "丸紅プラント", "industry": "商社", "region": "関東", "size": "大手", "segment_ids": ["SEG001", "SEG002"]},
    {"id": "A013", "name": "明治乳業", "industry": "食品", "region": "関東", "size": "大手", "segment_ids": ["SEG002"]},
    {"id": "A014", "name": "ダイキン工業", "industry": "空調", "region": "関西", "size": "大手", "segment_ids": ["SEG001"]},
    {"id": "A015", "name": "花王", "industry": "化学", "region": "関東", "size": "大手", "segment_ids": ["SEG002", "SEG003"]},
    {"id": "A016", "name": "クラレ", "industry": "化学", "region": "関西", "size": "中堅", "segment_ids": ["SEG001"]},
    {"id": "A017", "name": "カーボンテック", "industry": "環境", "region": "北海道", "size": "中堅", "segment_ids": ["SEG001"]},
    {"id": "A018", "name": "東レエンジニアリング", "industry": "繊維", "region": "関西", "size": "大手", "segment_ids": ["SEG002"]},
    {"id": "A019", "name": "住友化学", "industry": "化学", "region": "関西", "size": "大手", "segment_ids": ["SEG001", "SEG003"]},
    {"id": "A020", "name": "マレーシアペトロナス", "industry": "エネルギー", "region": "海外", "size": "大手", "segment_ids": ["SEG001"]},
    {"id": "A021", "name": "協和キリン", "industry": "医薬品", "region": "関東", "size": "大手", "segment_ids": ["SEG002"]},
    {"id": "A022", "name": "サントリープロダクツ", "industry": "飲料", "region": "関西", "size": "大手", "segment_ids": ["SEG002"]},
    {"id": "A023", "name": "日本製鉄", "industry": "鉄鋼", "region": "関東", "size": "大手", "segment_ids": ["SEG001"]},
    {"id": "A024", "name": "三菱重工", "industry": "重工業", "region": "関東", "size": "大手", "segment_ids": ["SEG001"]},
    {"id": "A025", "name": "中部電力", "industry": "電力", "region": "東海", "size": "大手", "segment_ids": ["SEG001"]},
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
    {"id": "SR10", "name": "松本新人", "segment_id": "SEG001", "years": 0, "role": "新任"},  # 4月異動の新任
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

CASE_TYPES = ["定期メンテナンス", "緊急修理", "部品交換", "技術問合せ", "改造・増設"]


def write_csv(filename, rows, fieldnames):
    path = OUTPUT_DIR / filename
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  {filename}: {len(rows)} rows")


def generate_dates():
    """Generate DIM_DATE for 2024-04-01 to 2026-09-30"""
    rows = []
    d = datetime(2024, 4, 1)
    end = datetime(2026, 9, 30)
    while d <= end:
        fy = d.year if d.month >= 4 else d.year - 1
        q = (d.month - 4) % 12 // 3 + 1 if d.month >= 4 else (d.month + 8) // 3
        rows.append({
            "DATE_KEY": d.strftime("%Y-%m-%d"),
            "YEAR": d.year,
            "MONTH": d.month,
            "DAY": d.day,
            "WEEKDAY": ["月", "火", "水", "木", "金", "土", "日"][d.weekday()],
            "IS_WEEKDAY": 1 if d.weekday() < 5 else 0,
            "FISCAL_YEAR": fy,
            "FISCAL_QUARTER": f"Q{q}",
            "FISCAL_YEAR_LABEL": f"{fy}年度",
        })
        d += timedelta(days=1)
    return rows


def generate_opportunities():
    """Generate FACT_OPPORTUNITY - realistic sales pipeline data"""
    rows = []
    opp_id = 1000

    # Generate opportunities per month
    start = datetime(2024, 4, 1)
    end = datetime(2026, 9, 30)
    d = start

    while d <= end:
        month_start = d.replace(day=1)
        if d.month == 12:
            month_end = d.replace(year=d.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            month_end = d.replace(month=d.month + 1, day=1) - timedelta(days=1)

        # Number of new opps per month varies by segment
        for seg in SEGMENTS:
            if seg["code"] == "HEX":
                n_opps = random.randint(8, 15)
            elif seg["code"] == "PE":
                n_opps = random.randint(3, 8)
            else:
                n_opps = random.randint(10, 20)

            seg_products = [p for p in PRODUCTS if p["segment_id"] == seg["id"]]
            seg_reps = [r for r in SALES_REPS if r["segment_id"] == seg["id"]]
            seg_accounts = [a for a in ACCOUNTS if seg["id"] in a["segment_ids"]]

            for _ in range(n_opps):
                opp_id += 1
                product = random.choice(seg_products)
                rep = random.choice(seg_reps)
                account = random.choice(seg_accounts)

                # Amount with variance
                base_amount = product["avg_price"]
                amount = int(base_amount * random.uniform(0.5, 2.0))

                # Create date within the month
                create_day = random.randint(1, min(28, month_end.day))
                create_date = month_start.replace(day=create_day)

                # Determine final stage based on time elapsed
                months_ago = (datetime(2026, 9, 30) - create_date).days / 30

                if months_ago > 6:
                    # Old opportunities should be closed
                    if random.random() < 0.45:
                        final_stage = 7  # Won
                        close_date = create_date + timedelta(days=random.randint(60, 180))
                    else:
                        if random.random() < 0.6:
                            final_stage = 8  # Lost
                            close_date = create_date + timedelta(days=random.randint(30, 120))
                        else:
                            # Still open (stale - problem!)
                            final_stage = random.randint(2, 6)
                            close_date = None
                elif months_ago > 3:
                    r = random.random()
                    if r < 0.3:
                        final_stage = 7
                        close_date = create_date + timedelta(days=random.randint(45, 120))
                    elif r < 0.45:
                        final_stage = 8
                        close_date = create_date + timedelta(days=random.randint(30, 90))
                    else:
                        final_stage = random.randint(3, 6)
                        close_date = None
                else:
                    # Recent - mostly in pipeline
                    final_stage = random.randint(1, 5)
                    close_date = None
                    if random.random() < 0.1:
                        final_stage = 7
                        close_date = create_date + timedelta(days=random.randint(14, 60))

                stage_name = STAGES[final_stage - 1]["name"]
                probability = STAGES[final_stage - 1]["probability"]

                # Days in current stage (longer = problem)
                if final_stage in (7, 8):
                    days_in_stage = 0
                elif months_ago > 6 and final_stage < 7:
                    days_in_stage = random.randint(60, 180)  # Stale!
                else:
                    days_in_stage = random.randint(3, STAGES[final_stage - 1]["avg_days"] * 2)

                # Expected close date
                if close_date:
                    expected_close = close_date
                else:
                    expected_close = create_date + timedelta(days=random.randint(60, 180))

                # Competitor
                competitor = random.choice(COMPETITORS)

                # New rep (SR10) issues: no activity, low conversion
                if rep["id"] == "SR10":
                    if final_stage >= 4:
                        final_stage = random.randint(1, 3)
                        stage_name = STAGES[final_stage - 1]["name"]
                        probability = STAGES[final_stage - 1]["probability"]
                    days_in_stage = int(days_in_stage * 1.5)

                # Cross-sell flag
                other_segs = [s for s in account.get("segment_ids", []) if s != seg["id"]]
                cross_sell_potential = 1 if other_segs else 0

                # Activity count (low = problem, visible in SF)
                if rep["years"] >= 10:
                    activity_count = random.randint(5, 15)
                elif rep["years"] >= 3:
                    activity_count = random.randint(3, 10)
                else:
                    activity_count = random.randint(0, 4)

                rows.append({
                    "OPP_ID": f"OPP-{opp_id}",
                    "DATE_KEY": create_date.strftime("%Y-%m-%d"),
                    "ACCOUNT_ID": account["id"],
                    "SEGMENT_ID": seg["id"],
                    "PRODUCT_ID": product["id"],
                    "SALES_REP_ID": rep["id"],
                    "OPP_NAME": f"{account['name']} {product['name']}",
                    "STAGE": stage_name,
                    "STAGE_ID": final_stage,
                    "AMOUNT": amount,
                    "PROBABILITY": probability,
                    "EXPECTED_CLOSE_DATE": expected_close.strftime("%Y-%m-%d"),
                    "CLOSE_DATE": close_date.strftime("%Y-%m-%d") if close_date else "",
                    "DAYS_IN_STAGE": days_in_stage,
                    "IS_WON": 1 if final_stage == 7 else 0,
                    "IS_LOST": 1 if final_stage == 8 else 0,
                    "IS_OPEN": 1 if final_stage < 7 else 0,
                    "COMPETITOR": competitor,
                    "CROSS_SELL_POTENTIAL": cross_sell_potential,
                    "ACTIVITY_COUNT": activity_count,
                    "CREATED_DATE": create_date.strftime("%Y-%m-%d"),
                })

        # Next month
        if d.month == 12:
            d = d.replace(year=d.year + 1, month=1, day=1)
        else:
            d = d.replace(month=d.month + 1, day=1)

    return rows


def generate_service_cases(opportunities):
    """Generate FACT_SERVICE_CASE based on won opportunities and installed base"""
    rows = []
    case_id = 5000

    # Build installed base from won opportunities
    won_opps = [o for o in opportunities if o["IS_WON"] == 1]

    for opp in won_opps:
        product = next((p for p in PRODUCTS if p["id"] == opp["PRODUCT_ID"]), None)
        if not product:
            continue

        # Generate maintenance cases based on product maintenance interval
        install_date = datetime.strptime(opp["CLOSE_DATE"], "%Y-%m-%d")
        interval = product["maint_interval_months"]
        case_date = install_date + timedelta(days=interval * 30)
        end_date = datetime(2026, 9, 30)

        while case_date <= end_date:
            case_id += 1

            # Case type distribution
            r = random.random()
            if r < 0.50:
                case_type = "定期メンテナンス"
                priority = "中"
                resolution_hours = random.randint(4, 24)
                revenue = int(opp["AMOUNT"] * random.uniform(0.02, 0.08))
            elif r < 0.70:
                case_type = "部品交換"
                priority = "中"
                resolution_hours = random.randint(8, 48)
                revenue = int(opp["AMOUNT"] * random.uniform(0.03, 0.10))
            elif r < 0.85:
                case_type = "技術問合せ"
                priority = "低"
                resolution_hours = random.randint(1, 8)
                revenue = 0
            elif r < 0.95:
                case_type = "緊急修理"
                priority = "高"
                resolution_hours = random.randint(2, 72)
                revenue = int(opp["AMOUNT"] * random.uniform(0.05, 0.15))
            else:
                case_type = "改造・増設"
                priority = "中"
                resolution_hours = random.randint(24, 160)
                revenue = int(opp["AMOUNT"] * random.uniform(0.10, 0.30))

            cost = int(revenue * random.uniform(0.3, 0.7)) if revenue > 0 else random.randint(10000, 100000)
            sla_target = 24 if priority == "高" else 72 if priority == "中" else 168
            sla_met = 1 if resolution_hours <= sla_target else 0
            satisfaction = random.choice([3, 4, 4, 4, 5, 5]) if sla_met else random.choice([1, 2, 2, 3, 3])

            # Status (recent cases may be open)
            days_ago = (end_date - case_date).days
            if days_ago < 14:
                status = random.choice(["新規", "対応中", "対応中", "完了"])
            elif days_ago < 7:
                status = random.choice(["新規", "対応中"])
            else:
                status = "完了"

            rows.append({
                "CASE_ID": f"CS-{case_id}",
                "DATE_KEY": case_date.strftime("%Y-%m-%d"),
                "ACCOUNT_ID": opp["ACCOUNT_ID"],
                "SEGMENT_ID": opp["SEGMENT_ID"],
                "PRODUCT_ID": opp["PRODUCT_ID"],
                "RELATED_OPP_ID": opp["OPP_ID"],
                "CASE_TYPE": case_type,
                "PRIORITY": priority,
                "STATUS": status,
                "RESOLUTION_HOURS": resolution_hours if status == "完了" else None,
                "SLA_TARGET_HOURS": sla_target,
                "SLA_MET": sla_met if status == "完了" else None,
                "REVENUE": revenue,
                "COST": cost,
                "GROSS_PROFIT": revenue - cost,
                "SATISFACTION_SCORE": satisfaction if status == "完了" else None,
            })

            # Next maintenance
            jitter = random.randint(-15, 30)
            case_date += timedelta(days=interval * 30 + jitter)

    # Add some random emergency cases not tied to won opps
    for _ in range(50):
        case_id += 1
        account = random.choice(ACCOUNTS)
        seg_id = random.choice(account["segment_ids"])
        seg_products = [p for p in PRODUCTS if p["segment_id"] == seg_id]
        product = random.choice(seg_products)
        case_date = datetime(2024, 4, 1) + timedelta(days=random.randint(0, 900))
        if case_date > datetime(2026, 9, 30):
            continue
        revenue = int(product["avg_price"] * random.uniform(0.05, 0.15))
        cost = int(revenue * random.uniform(0.4, 0.8))
        rows.append({
            "CASE_ID": f"CS-{case_id}",
            "DATE_KEY": case_date.strftime("%Y-%m-%d"),
            "ACCOUNT_ID": account["id"],
            "SEGMENT_ID": seg_id,
            "PRODUCT_ID": product["id"],
            "RELATED_OPP_ID": "",
            "CASE_TYPE": "緊急修理",
            "PRIORITY": "高",
            "STATUS": "完了",
            "RESOLUTION_HOURS": random.randint(4, 72),
            "SLA_TARGET_HOURS": 24,
            "SLA_MET": random.choice([0, 0, 1, 1, 1]),
            "REVENUE": revenue,
            "COST": cost,
            "GROSS_PROFIT": revenue - cost,
            "SATISFACTION_SCORE": random.choice([2, 3, 3, 4]),
        })

    return rows


def main():
    print("=== 日阪製作所 デモデータ生成 ===\n")

    # DIM tables
    print("Dimension tables:")
    write_csv("dim_segment.csv", SEGMENTS,
              ["id", "name", "code", "revenue_ratio"])

    write_csv("dim_product.csv",
              [{"id": p["id"], "name": p["name"], "segment_id": p["segment_id"],
                "category": p["category"], "avg_price": p["avg_price"],
                "maint_interval_months": p["maint_interval_months"]} for p in PRODUCTS],
              ["id", "name", "segment_id", "category", "avg_price", "maint_interval_months"])

    write_csv("dim_account.csv",
              [{"id": a["id"], "name": a["name"], "industry": a["industry"],
                "region": a["region"], "size": a["size"],
                "segment_ids": "|".join(a["segment_ids"])} for a in ACCOUNTS],
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

    # Summary
    print(f"\n=== 生成完了 ===")
    print(f"商談数: {len(opportunities)}")
    print(f"  受注: {sum(1 for o in opportunities if o['IS_WON'])}")
    print(f"  失注: {sum(1 for o in opportunities if o['IS_LOST'])}")
    print(f"  進行中: {sum(1 for o in opportunities if o['IS_OPEN'])}")
    print(f"サービス案件数: {len(cases)}")
    print(f"出力先: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
