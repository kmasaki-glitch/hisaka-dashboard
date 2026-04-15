"""
Hisaka Demo - Snowflake Auto Deploy Script
===========================================
snowflake-connector-python + JWT private key authentication.
Uploads CSV via PUT, loads via COPY INTO, executes SQL files in order.

Usage:
  python deploy.py
"""

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"
SQL_DIR = OUTPUT_DIR / "sql"
CSV_DIR = OUTPUT_DIR / "csv"
REPORT_PATH = OUTPUT_DIR / "deploy_report.json"

# Snowflake connection config
ACCOUNT = "rjgcbvq-ke67766"
USER = "KMASAKI"
ROLE = "ACCOUNTADMIN"
WAREHOUSE = "HISAKA_DEMO_WH"
DATABASE = "HISAKA_DEMO_DB"
SCHEMA = "SALES"
PRIVATE_KEY_PATH = os.path.expanduser("~/.snowflake/keys/snowflake_private_key.p8")

STAGE_NAME = "HISAKA_CSV_STAGE"

# Table load order (dimensions first, then facts)
TABLES = [
    "DIM_SEGMENT",
    "DIM_PRODUCT",
    "DIM_ACCOUNT",
    "DIM_SALES_REP",
    "DIM_STAGE",
    "DIM_DATE",
    "FACT_OPPORTUNITY",
    "FACT_SERVICE_CASE",
]


def load_private_key():
    with open(PRIVATE_KEY_PATH, "rb") as f:
        p_key = serialization.load_pem_private_key(
            f.read(), password=None, backend=default_backend()
        )
    return p_key


def get_connection():
    return snowflake.connector.connect(
        account=ACCOUNT,
        user=USER,
        private_key=load_private_key(),
        role=ROLE,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
    )


class DeployReport:
    def __init__(self):
        self.steps = []
        self.start_time = datetime.now()

    def add(self, step, status, detail="", rows=0):
        entry = {
            "step": step,
            "status": status,
            "detail": detail,
            "rows": rows,
            "timestamp": datetime.now().isoformat(),
        }
        self.steps.append(entry)
        icon = "OK" if status == "success" else "FAIL" if status == "error" else "SKIP"
        print(f"  [{icon}] {step}: {detail}")
        return status == "success"

    def save(self):
        report = {
            "start": self.start_time.isoformat(),
            "end": datetime.now().isoformat(),
            "duration_sec": (datetime.now() - self.start_time).total_seconds(),
            "total_steps": len(self.steps),
            "success": sum(1 for s in self.steps if s["status"] == "success"),
            "errors": sum(1 for s in self.steps if s["status"] == "error"),
            "steps": self.steps,
        }
        REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2))
        print(f"\nReport saved: {REPORT_PATH}")
        return report


def execute_sql(conn, sql, step_name, report):
    """Execute a single SQL statement and report result."""
    try:
        cur = conn.cursor()
        cur.execute(sql)
        result = cur.fetchall()
        report.add(step_name, "success", f"{len(result)} rows returned")
        return result
    except Exception as e:
        report.add(step_name, "error", str(e)[:200])
        return None


def execute_sql_file(conn, filepath, report):
    """Execute a SQL file containing multiple statements."""
    sql_text = filepath.read_text(encoding="utf-8")
    # Split by semicolons, ignoring those inside strings/comments
    statements = []
    current = []
    in_dollar = False
    for line in sql_text.split("\n"):
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        if "$$" in line:
            in_dollar = not in_dollar
        current.append(line)
        if not in_dollar and ";" in line:
            stmt = "\n".join(current).strip()
            if stmt and stmt != ";":
                statements.append(stmt)
            current = []
    if current:
        stmt = "\n".join(current).strip()
        if stmt:
            statements.append(stmt)

    success_count = 0
    for i, stmt in enumerate(statements):
        stmt_clean = stmt.rstrip().rstrip(";")
        if not stmt_clean or stmt_clean.isspace():
            continue
        step = f"{filepath.name} [{i+1}/{len(statements)}]"
        try:
            cur = conn.cursor()
            cur.execute(stmt_clean)
            cur.fetchall()
            success_count += 1
        except Exception as e:
            err_msg = str(e)[:200]
            report.add(step, "error", err_msg)
            print(f"    SQL: {stmt_clean[:100]}...")

    report.add(
        f"{filepath.name} complete",
        "success" if success_count == len(statements) else "partial",
        f"{success_count}/{len(statements)} statements succeeded",
    )
    return success_count


def step_1_setup(conn, report):
    """Create Database, Schema, Warehouse, Stage."""
    print("\n[Step 1] Setup: DB / Schema / Warehouse / Stage")

    setup_sqls = [
        f"USE ROLE {ROLE}",
        f"CREATE DATABASE IF NOT EXISTS {DATABASE}",
        f"USE DATABASE {DATABASE}",
        f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
        f"USE SCHEMA {SCHEMA}",
        f"CREATE WAREHOUSE IF NOT EXISTS {WAREHOUSE} WITH WAREHOUSE_SIZE='XSMALL' AUTO_SUSPEND=60 AUTO_RESUME=TRUE",
        f"USE WAREHOUSE {WAREHOUSE}",
        f"CREATE OR REPLACE STAGE {STAGE_NAME} FILE_FORMAT=(TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='\"' SKIP_HEADER=1 FIELD_DELIMITER=',' NULL_IF=(''))",
    ]
    for sql in setup_sqls:
        execute_sql(conn, sql, " ".join(sql.split()[:4]), report)


def step_2_create_tables(conn, report):
    """Create all tables from DDL file."""
    print("\n[Step 2] Create Tables (8 tables)")
    ddl_file = SQL_DIR / "01_create_tables.sql"
    execute_sql_file(conn, ddl_file, report)


def step_3_upload_csv(conn, report):
    """Upload CSV files to stage using PUT."""
    print("\n[Step 3] Upload CSV to Stage")
    csv_files = sorted(CSV_DIR.glob("*.csv"))
    for csv_file in csv_files:
        step = f"PUT {csv_file.name}"
        try:
            cur = conn.cursor()
            cur.execute(
                f"PUT file://{csv_file} @{STAGE_NAME} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )
            result = cur.fetchall()
            status = result[0][6] if result else "unknown"
            report.add(step, "success", status)
        except Exception as e:
            report.add(step, "error", str(e)[:200])


def step_4_load_data(conn, report):
    """COPY INTO tables and verify row counts."""
    print("\n[Step 4] Load Data (COPY INTO)")

    for table in TABLES:
        csv_name = table.lower() + ".csv"
        step = f"COPY INTO {table}"
        try:
            cur = conn.cursor()
            cur.execute(f"""
                COPY INTO {table}
                FROM @{STAGE_NAME}/{csv_name}
                FILE_FORMAT=(TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1 FIELD_DELIMITER=',' NULL_IF=(''))
                ON_ERROR='CONTINUE'
            """)
            result = cur.fetchall()
            rows_loaded = result[0][2] if result else 0
            errors = result[0][5] if result else 0
            report.add(step, "success", f"{rows_loaded} rows loaded, {errors} errors")
        except Exception as e:
            report.add(step, "error", str(e)[:200])

    # Verify row counts
    print("\n  Verifying row counts...")
    for table in TABLES:
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            report.add(f"VERIFY {table}", "success", f"{count} rows")
        except Exception as e:
            report.add(f"VERIFY {table}", "error", str(e)[:200])


def step_5_create_views(conn, report):
    """Create analytics views with Japanese column names."""
    print("\n[Step 5] Create Views (10 views)")
    view_file = SQL_DIR / "03_views.sql"
    execute_sql_file(conn, view_file, report)

    # Verify
    try:
        cur = conn.cursor()
        cur.execute(f"SHOW VIEWS IN SCHEMA {SCHEMA}")
        views = cur.fetchall()
        view_names = [v[1] for v in views]
        report.add(
            "VERIFY Views", "success", f"{len(view_names)} views: {', '.join(view_names)}"
        )
    except Exception as e:
        report.add("VERIFY Views", "error", str(e)[:200])


def step_6_verify(conn, report):
    """Run sample queries to verify deployment."""
    print("\n[Step 6] Verification Queries")

    test_queries = [
        ("Pipeline summary count", "SELECT COUNT(*) FROM V_PIPELINE_SUMMARY"),
        ("Sales forecast count", "SELECT COUNT(*) FROM V_SALES_FORECAST"),
        ("Rep performance count", "SELECT COUNT(*) FROM V_REP_PERFORMANCE"),
        (
            "Top segment by won amount",
            'SELECT "事業部名", SUM("受注金額合計") AS total FROM V_SEGMENT_COMPARISON GROUP BY "事業部名" ORDER BY total DESC LIMIT 3',
        ),
        (
            "Stale opportunities",
            "SELECT COUNT(*) FROM V_STALE_OPPORTUNITIES",
        ),
        (
            "Service-to-sales potential",
            'SELECT COUNT(*) FROM V_SERVICE_TO_SALES WHERE "営業ステータス" = \'オープン商談なし - 要アプローチ\'',
        ),
    ]

    for name, sql in test_queries:
        try:
            cur = conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            if result:
                detail = str(result[0]) if len(result) == 1 else f"{len(result)} rows"
                report.add(f"TEST: {name}", "success", detail)
            else:
                report.add(f"TEST: {name}", "success", "0 rows")
        except Exception as e:
            report.add(f"TEST: {name}", "error", str(e)[:200])


def main():
    print("=" * 60)
    print("  Hisaka Demo - Snowflake Auto Deploy")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    report = DeployReport()

    # Connect
    print("\nConnecting to Snowflake...")
    try:
        conn = get_connection()
        report.add("Connection", "success", f"{ACCOUNT} / {USER}")
    except Exception as e:
        report.add("Connection", "error", str(e)[:200])
        print(f"\nFATAL: Cannot connect to Snowflake: {e}")
        report.save()
        sys.exit(1)

    try:
        step_1_setup(conn, report)
        step_2_create_tables(conn, report)
        step_3_upload_csv(conn, report)
        step_4_load_data(conn, report)
        step_5_create_views(conn, report)
        step_6_verify(conn, report)
    except Exception as e:
        report.add("FATAL", "error", str(e)[:500])
    finally:
        conn.close()

    # Summary
    result = report.save()
    print(f"\n{'=' * 60}")
    print(f"  Deploy Complete: {result['success']}/{result['total_steps']} steps succeeded")
    if result["errors"] > 0:
        print(f"  Errors: {result['errors']}")
        for s in result["steps"]:
            if s["status"] == "error":
                print(f"    - {s['step']}: {s['detail'][:100]}")
    print(f"  Duration: {result['duration_sec']:.1f}s")
    print(f"{'=' * 60}")

    sys.exit(1 if result["errors"] > 0 else 0)


if __name__ == "__main__":
    main()
