"""fetch_repeat.py — Re-fetch ONLY the repeat WO data from BigQuery.

Writes: data/all_fm_wos.json
"""
import json
import os
import subprocess
import sys
from pathlib import Path

# Use the same project as the main dashboard
BILLING_PROJECT = "re-ods-explorer"
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)


def _strip_comments(s):
    """Strip SQL -- line comments to prevent collapse bugs."""
    lines = []
    for line in s.splitlines():
        idx = line.find("--")
        lines.append(line[:idx] if idx >= 0 else line)
    return "\n".join(lines)


def run_query(name, sql, max_rows=500_000):
    """Execute a BQ query and save results as JSON."""
    out_path = DATA_DIR / f"{name}.json"
    sql_clean = " ".join(_strip_comments(sql).split())
    cmd = [
        "bq", "query",
        f"--project_id={BILLING_PROJECT}",
        "--use_legacy_sql=false",
        "--format=json",
        f"--max_rows={max_rows}",
        "--quiet",
        sql_clean,
    ]
    print(f"→ Fetching {name}...")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            print(f"  ❌ BQ error: {result.stderr[-1000:]}", file=sys.stderr)
            return []

        # Robust JSON parsing: find start of array
        out_str = result.stdout
        start_idx = out_str.find("[")
        if start_idx != -1:
            out_str = out_str[start_idx:]
        else:
            # If no array start found, maybe empty or error
            if not out_str.strip():
                out_str = "[]"
            # If content exists but no [, try parsing anyway (maybe single object?)
            # Or just let json.loads fail

        rows = json.loads(out_str)
        with open(out_path, "w") as f:
            json.dump(rows, f)
        print(f"  ✅ {name}: {len(rows):,} rows → {out_path}")
        return rows
    except json.JSONDecodeError as e:
        print(f"  ❌ JSON decode error: {e}", file=sys.stderr)
        print(f"  --- stdout start ---\n{result.stdout[:300]}\n--- stdout end ---", file=sys.stderr)
        return []
    except subprocess.TimeoutExpired:
        print(f"  ❌ Timed out after 300s", file=sys.stderr)
        return []
    except Exception as e:
        print(f"  ❌ Unexpected error: {e}", file=sys.stderr)
        return []


def fetch_repeat_wos():
    return run_query("all_fm_wos", """
        WITH ranked AS (
            SELECT 
                r.store_nbr, 
                r.problem_type_desc, 
                r.problem_code_desc, 
                r.sc_trade_name, 
                r.category_name,
                CAST(r.tracking_nbr AS STRING) AS tracking_nbr,
                CAST(r.completion_date AS DATE) AS completion_date,
                r.not_to_exceed_amt,
                COALESCE(r.equipment_desc, '')          AS equipment_desc,
                COALESCE(r.equipment_tagid, '')         AS equipment_tagid,
                
                -- Hierarchy from ODS (fsai_workorders has accurate current alignment)
                COALESCE(w.fm_director, '')             AS fm_director,
                COALESCE(w.fm_sr_director, '')          AS sr_director,
                COALESCE(w.fm_regional_mgr, '')         AS regional_mgr,
                COALESCE(w.aligned_fs_mgr, '')          AS fs_mgr,
                
                SUBSTR(COALESCE(r.problem_desc, ''), 1, 200) AS problem_desc,
                LAG(CAST(r.completion_date AS DATE)) OVER (
                    PARTITION BY r.store_nbr, r.problem_type_desc, r.problem_code_desc
                    ORDER BY r.completion_date
                ) AS prev_completion
            FROM `re-crystal-mdm-prod.crystal.sc_workorder` r
            LEFT JOIN `re-ods-explorer.us_re_fm_prod.fsai_workorders` w
              ON CAST(r.tracking_nbr AS STRING) = CAST(w.tracking_nbr AS STRING)
            WHERE r.completion_date IS NOT NULL
              AND r.completion_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
              AND r.store_nbr IS NOT NULL
              AND UPPER(r.sc_trade_name) LIKE 'FM %'
              AND UPPER(COALESCE(r.problem_type_desc,'')) NOT IN (
                  'PM','SCHEDULED SERVICE','PREVENTIVE MAINTENANCE',
                  'PEST CONTROL','NONE',''
              )
        ),
        repeat_pairs AS (
            SELECT DISTINCT store_nbr, problem_type_desc, problem_code_desc
            FROM ranked
            WHERE prev_completion IS NOT NULL
              AND DATE_DIFF(completion_date, prev_completion, DAY) <= 30
        ),
        combined AS (
            SELECT r.store_nbr, r.problem_type_desc, r.problem_code_desc,
                r.sc_trade_name, r.category_name,
                r.tracking_nbr, r.completion_date, r.not_to_exceed_amt,
                r.equipment_desc, r.equipment_tagid, r.problem_desc,
                r.fm_director, r.sr_director, r.regional_mgr, r.fs_mgr,
                CASE WHEN r.prev_completion IS NOT NULL
                     THEN DATE_DIFF(r.completion_date, r.prev_completion, DAY)
                END AS days_since_prev
            FROM ranked r
            JOIN repeat_pairs rp
                ON r.store_nbr         = rp.store_nbr
               AND r.problem_type_desc = rp.problem_type_desc
               AND COALESCE(r.problem_code_desc, '') = COALESCE(rp.problem_code_desc, '')
        ),
        asset_counts AS (
            SELECT store_nbr, problem_type_desc, problem_code_desc,
                NULLIF(TRIM(equipment_desc),  '') AS equipment_desc,
                NULLIF(TRIM(equipment_tagid), '') AS equipment_tagid,
                COUNT(*) AS wo_count
            FROM combined
            WHERE NULLIF(TRIM(equipment_desc), '') IS NOT NULL
            GROUP BY store_nbr, problem_type_desc, problem_code_desc,
                     equipment_desc, equipment_tagid
        ),
        asset_counts_agg AS (
            SELECT store_nbr, problem_type_desc, problem_code_desc,
                STRING_AGG(
                    CONCAT(
                        equipment_desc, '~',
                        COALESCE(equipment_tagid, ''), '~',
                        CAST(wo_count AS STRING)
                    )
                    ORDER BY wo_count DESC LIMIT 6
                ) AS asset_counts_str
            FROM asset_counts
            GROUP BY store_nbr, problem_type_desc, problem_code_desc
        ),
        main_agg AS (
            SELECT
                store_nbr, problem_type_desc, problem_code_desc, sc_trade_name, category_name,
                MAX(fm_director)                                      AS fm_director,
                MAX(sr_director)                                      AS sr_director,
                MAX(regional_mgr)                                     AS regional_mgr,
                MAX(fs_mgr)                                           AS fs_mgr,
                COUNT(*)                                              AS completion_count,
                CAST(MIN(completion_date) AS STRING)                 AS first_completion,
                CAST(MAX(completion_date) AS STRING)                 AS last_completion,
                ROUND(SUM(COALESCE(not_to_exceed_amt, 0)), 2)       AS total_nte,
                ROUND(AVG(COALESCE(not_to_exceed_amt, 0)), 2)       AS avg_nte,
                MIN(days_since_prev)                                 AS min_days_between,
                SUM(CASE WHEN days_since_prev IS NOT NULL
                              AND days_since_prev <= 30
                         THEN 1 ELSE 0 END)                         AS repeat_count_30d,
                STRING_AGG(tracking_nbr ORDER BY completion_date DESC LIMIT 20)
                                                                    AS tracking_numbers,
                STRING_AGG(problem_desc ORDER BY completion_date DESC LIMIT 3)
                                                                    AS sample_descs_raw
            FROM combined
            GROUP BY store_nbr, problem_type_desc, problem_code_desc, sc_trade_name, category_name
            HAVING SUM(CASE WHEN days_since_prev IS NOT NULL
                                 AND days_since_prev <= 30
                            THEN 1 ELSE 0 END) >= 2
        )
        SELECT m.*, a.asset_counts_str
        FROM main_agg m
        LEFT JOIN asset_counts_agg a
               ON m.store_nbr         = a.store_nbr
              AND m.problem_type_desc = a.problem_type_desc
              AND COALESCE(m.problem_code_desc, '') = COALESCE(a.problem_code_desc, '')
        ORDER BY m.repeat_count_30d DESC, m.total_nte DESC
    """, max_rows=500_000)

if __name__ == "__main__":
    fetch_repeat_wos()
