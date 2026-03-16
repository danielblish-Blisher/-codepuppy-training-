"""fetch_data.py — Pull all-trades training gap data from BigQuery.

Queries re-ods-explorer.us_re_fm_prod for:
  - Repeat WOs by trade          → data/repeats_by_trade.json
  - Top problem codes (gaps)     → data/problem_codes.json
  - Store hotspots               → data/store_hotspots.json
  - Monthly trend                → data/monthly_trend.json
  - Repeat WO detail list        → data/repeat_detail.json

Usage:
    .venv/bin/python fetch_data.py
"""
import json
import os
import subprocess
import sys
from datetime import datetime

BILLING_PROJECT = "re-ods-explorer"
DATA_DIR = "data"
MAX_ROWS = 500_000

os.makedirs(DATA_DIR, exist_ok=True)


def run_query(name: str, sql: str, max_rows: int = MAX_ROWS) -> bool:
    """Run a BQ query via the CLI, save results as JSON, return success."""
    print(f"  → {name}...")

    def _strip_comments(s):
        lines = []
        for line in s.splitlines():
            idx = line.find("--")
            lines.append(line[:idx] if idx >= 0 else line)
        return "\n".join(lines)

    sql_clean = " ".join(_strip_comments(sql).split())
    cmd = [
        "bq", "query",
        "--use_legacy_sql=false",
        f"--project_id={BILLING_PROJECT}",
        "--format=json",
        f"--max_rows={max_rows}",
        sql_clean,
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=300
        )
        if result.returncode != 0:
            print(f"  ❌ {name}: {result.stderr.strip()[:200]}")
            return False

        stdout = result.stdout.strip()
        json_start = stdout.find("[")
        if json_start == -1:
            print(f"  ❌ {name}: no JSON in output")
            return False

        rows = json.loads(stdout[json_start:])
        out_path = os.path.join(DATA_DIR, f"{name}.json")
        with open(out_path, "w") as f:
            json.dump(rows, f, indent=2, default=str)
        print(f"  ✅ {name}: {len(rows)} rows")
        return True

    except subprocess.TimeoutExpired:
        print(f"  ❌ {name}: timed out (>300s)")
        return False
    except Exception as e:
        print(f"  ❌ {name}: {e}")
        return False


# ── Query 1: Repeat WOs by Trade ─────────────────────────────────────────
REPEATS_BY_TRADE = """
SELECT
  COALESCE(sc_trade_name, 'Unknown') AS trade,
  COUNT(*)                            AS total_repeats,
  COUNT(DISTINCT store_nbr)           AS stores_affected,
  COUNT(DISTINCT equip_tagid)         AS assets_affected,
  SUM(month3_flag)                    AS repeats_90d,
  SUM(month6_flag)                    AS repeats_180d,
  SUM(month12_flag)                   AS repeats_365d
FROM `re-ods-explorer.us_re_fm_prod.fsai_recurring_damaged_assets`
WHERE call_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
  AND sc_trade_name IS NOT NULL
GROUP BY trade
ORDER BY total_repeats DESC
LIMIT 40
"""

# ── Query 2: Top Problem Codes (training gap signals) ────────────────────
PROBLEM_CODES = """
SELECT
  COALESCE(sc_trade_name, 'Unknown')      AS trade,
  COALESCE(problem_code_desc, 'Unknown')  AS problem_code,
  COUNT(*)                                 AS repeat_count,
  COUNT(DISTINCT store_nbr)                AS stores_affected,
  SUM(month3_flag)                         AS within_90d,
  SUM(month6_flag)                         AS within_180d
FROM `re-ods-explorer.us_re_fm_prod.fsai_recurring_damaged_assets`
WHERE call_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
  AND sc_trade_name IS NOT NULL
GROUP BY trade, problem_code
HAVING repeat_count >= 3
ORDER BY repeat_count DESC
LIMIT 100
"""

# ── Query 3: Store Hotspots ───────────────────────────────────────────────
STORE_HOTSPOTS = """
SELECT
  r.store_nbr,
  MAX(w.store_city)       AS city,
  MAX(w.store_state)      AS state,
  MAX(w.fm_sr_director)   AS sr_director,
  MAX(w.fm_director)      AS fm_director,
  MAX(w.store_type_name)  AS banner,
  COUNT(*)                AS total_repeats,
  COUNT(DISTINCT r.sc_trade_name) AS trades_affected,
  SUM(r.month3_flag)      AS repeats_90d,
  SUM(r.month6_flag)      AS repeats_180d,
  STRING_AGG(
    DISTINCT r.sc_trade_name,
    ', '
    LIMIT 5
  )                       AS top_trades
FROM `re-ods-explorer.us_re_fm_prod.fsai_recurring_damaged_assets` r
LEFT JOIN `re-ods-explorer.us_re_fm_prod.fsai_workorders` w
  ON r.tracking_nbr = w.tracking_nbr
WHERE r.call_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
  AND r.month3_flag = 1
GROUP BY r.store_nbr
ORDER BY total_repeats DESC
LIMIT 100
"""

# ── Query 4: Monthly Trend ────────────────────────────────────────────────
MONTHLY_TREND = """
SELECT
  FORMAT_DATE('%Y-%m', call_date) AS month,
  COALESCE(sc_trade_name, 'Unknown') AS trade,
  COUNT(*)                         AS total_repeats,
  SUM(month3_flag)                 AS within_90d,
  COUNT(DISTINCT store_nbr)        AS stores_affected
FROM `re-ods-explorer.us_re_fm_prod.fsai_recurring_damaged_assets`
WHERE call_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
  AND sc_trade_name IS NOT NULL
GROUP BY month, trade
ORDER BY month ASC, total_repeats DESC
"""

# ── Query 5: Repeat WO Detail List (no join — fast) ─────────────────────
REPEAT_DETAIL = """
SELECT
  r.tracking_nbr,
  r.prev_tracking_nbr,
  r.store_nbr,
  COALESCE(r.sc_trade_name, '')        AS trade,
  COALESCE(r.equip_tagid,  '')          AS asset_tag,
  COALESCE(r.asset_type,   '')          AS asset_type,
  CAST(r.call_date AS STRING)           AS call_date,
  CAST(r.prev_call_date AS STRING)      AS prev_call_date,
  DATE_DIFF(r.call_date, r.prev_call_date, DAY) AS days_between,
  COALESCE(r.problem_code_desc, '')     AS problem_code,
  COALESCE(r.prev_problem_code_desc,'') AS prev_problem_code,
  r.month3_flag,
  r.month6_flag,
  r.month12_flag
FROM `re-ods-explorer.us_re_fm_prod.fsai_recurring_damaged_assets` r
WHERE r.call_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
  AND r.month3_flag = 1
  AND r.sc_trade_name IS NOT NULL
ORDER BY r.call_date DESC
LIMIT 10000
"""

# ── Query 6: Tech-level Repeat Stats ─────────────────────────────────────
TECH_REPEATS = """
SELECT
  COALESCE(w.latest_activity_tech, 'Unknown') AS tech_name,
  COALESCE(r.sc_trade_name, 'Unknown')        AS trade,
  COUNT(*)                                     AS repeat_wos,
  COUNT(DISTINCT r.store_nbr)                  AS stores,
  COUNT(DISTINCT r.equip_tagid)                AS unique_assets,
  SUM(r.month3_flag)                           AS within_90d
FROM `re-ods-explorer.us_re_fm_prod.fsai_recurring_damaged_assets` r
LEFT JOIN `re-ods-explorer.us_re_fm_prod.fsai_workorders` w
  ON r.tracking_nbr = w.tracking_nbr
WHERE r.call_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
  AND r.month3_flag = 1
  AND w.latest_activity_tech IS NOT NULL
  AND w.latest_activity_tech NOT IN ('', 'None')
GROUP BY tech_name, trade
HAVING repeat_wos >= 2
ORDER BY repeat_wos DESC
LIMIT 200
"""


def main():
    print("\n" + "="*60)
    print("📊 FM Trades Training Gap — BQ Data Fetch")
    print("="*60)
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"   Started: {ts}")

    results = [
        run_query("repeats_by_trade", REPEATS_BY_TRADE),
        run_query("problem_codes",    PROBLEM_CODES),
        run_query("store_hotspots",   STORE_HOTSPOTS),
        run_query("monthly_trend",    MONTHLY_TREND),
        run_query("repeat_detail",    REPEAT_DETAIL),
        run_query("tech_repeats",     TECH_REPEATS),
    ]

    ok  = sum(results)
    total = len(results)
    print(f"\n{'='*60}")
    print(f"  Done: {ok}/{total} queries succeeded")
    print("="*60)
    return 0 if ok == total else 1


if __name__ == "__main__":
    sys.exit(main())
