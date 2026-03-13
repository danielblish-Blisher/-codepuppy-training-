"""Fetch HVACR Training Dashboard data from BigQuery via bq CLI.

Queries:
  - doc_exam_reg            : exam registrations + pass/fail/scores
  - epa_certs               : EPA 608 cert compliance + expiry
  - tech_alignment          : technician roster, org hierarchy, store assignments
  - training_workorders     : HVACR WOs with FCR, SLA, trip count, repeat flags
  - observations            : FSAI field coaching / survey observations
  - top_gun                 : Top-Gun program before/after outcomes
  - pm_compliance           : CMMS preventive maintenance compliance

Billing: re-ods-explorer (same as HVACR dashboard)
"""
import json
import os
import subprocess
import sys

BILLING = "re-ods-explorer"
DATA_DIR = "data"
MAX_ROWS = 300_000

os.makedirs(DATA_DIR, exist_ok=True)


# ── Shared helpers ────────────────────────────────────────────

def _strip_comments(sql: str) -> str:
    """Remove -- line comments so bq CLI doesn't choke."""
    lines = []
    for line in sql.splitlines():
        idx = line.find("--")
        lines.append(line[:idx] if idx >= 0 else line)
    return "\n".join(lines)


def run_query(name: str, sql: str, max_rows: int = MAX_ROWS) -> bool:
    """Execute a BQ query via CLI, persist rows to data/<name>.json."""
    print(f"\n{'─'*60}")
    print(f"  Fetching {name}...")
    clean = " ".join(_strip_comments(sql).split())
    cmd = [
        "bq", "query",
        "--use_legacy_sql=false",
        f"--project_id={BILLING}",
        "--format=json",
        f"--max_rows={max_rows}",
        clean,
    ]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if res.returncode != 0:
            print(f"  ❌ {name}:\n{res.stderr.strip()}")
            return False
        stdout = res.stdout.strip()
        start = stdout.find("[")
        if start == -1:
            print(f"  ❌ {name}: no JSON in output")
            return False
        rows = json.loads(stdout[start:])
        out = os.path.join(DATA_DIR, f"{name}.json")
        with open(out, "w") as f:
            json.dump(rows, f, indent=2, default=str)
        print(f"  ✅ {name}: {len(rows):,} rows → {out}")
        return True
    except subprocess.TimeoutExpired:
        print(f"  ❌ {name}: query timed out (>300s)")
        return False
    except Exception as exc:
        print(f"  ❌ {name}: {exc}")
        return False


# ── Queries ───────────────────────────────────────────────────

run_query("exam_reg", """
    SELECT
        student_id,
        student_name,
        student_email,
        student_department,
        student_job_title,
        test_type,
        exam_proctor_name,
        Testing_Date,
        Score,
        Current_Tech_Level,
        CAST(Attempt AS STRING)   AS attempt,
        Result,
        Are_you_a_remote_associate AS is_remote,
        wmt_year_nbr,
        wmt_week_of_year_nbr
    FROM `re-ods-explorer.us_re_fm_prod.doc_exam_reg`
    ORDER BY Testing_Date DESC
""")


# NOTE: epa_cert_audit_reports_full references us_re_fm_stage.epa_cert_audit_reports_agg
# which requires additional permissions. Attempting query; will fall back to empty.
ok = run_query("epa_certs", """
    SELECT
        full_name,
        user_id,
        org_role,
        user_status,
        walmart_hire_date,
        in_source_start_date,
        license_category,
        license_type,
        license_number,
        issue_date,
        expiry_date,
        license_verified,
        uploaded_by
    FROM `re-ods-explorer.us_re_fm_prod.epa_cert_audit_reports_full`
    ORDER BY full_name
""")
if not ok:
    import json as _json, os as _os
    _path = _os.path.join(DATA_DIR, "epa_certs.json")
    if not _os.path.exists(_path) or _os.path.getsize(_path) < 10:
        with open(_path, "w") as _f:
            _json.dump([], _f)
    print("  ⚠️  epa_certs: no access to staging table — EPA tab will be empty")


run_query("tech_alignment", """
    SELECT
        user_id,
        first_name,
        last_name,
        status,
        org_role,
        technician_level,
        home_store,
        walmart_hire_date,
        in_source_start_date,
        mgr_first_name,
        mgr_last_name,
        mgr_org_role,
        mgr_technician_level,
        store_count
    FROM `re-ods-explorer.us_re_fm_prod.fsai_tech_alignment`
    WHERE status LIKE '%Active%'
    ORDER BY last_name, first_name
""")


# Tech-to-store mapping (unnested)
run_query("tech_stores", """
    SELECT
        t.user_id,
        CONCAT(t.first_name, ' ', t.last_name) AS tech_name,
        t.technician_level,
        t.org_role,
        t.status,
        s.store_nbr,
        s.store_type_name                  AS store_type,
        s.delivery_model,
        s.fm_regional_manager_name         AS rm,
        s.fm_director_name                 AS director,
        s.fm_sr_director_name              AS sr_director
    FROM `re-ods-explorer.us_re_fm_prod.fsai_tech_alignment` t,
    UNNEST(t.store_info) AS s
    WHERE t.status LIKE '%Active%'
    ORDER BY t.last_name, s.store_nbr
""", max_rows=500_000)


run_query("training_workorders", """
    SELECT
        wo.tracking_nbr,
        wo.store_nbr,
        wo.trade_name,
        wo.problem_code_desc,
        wo.status_extended_name             AS status_name,
        wo.completion_date,
        wo.nte                              AS not_to_exceed_amt,
        wo.trip_count,
        wo.total_repair_minutes,
        wo.sla_response_compliance,
        wo.sla_repair_compliance,
        wo.first_time_fix_compliance,
        wo.orig_hvacr_tech,
        wo.latest_activity_tech,
        wo.latest_activity_tech_org_role,
        wo.fm_sr_director,
        wo.fm_director,
        wo.fm_regional_mgr,
        wo.store_type_name
    FROM `re-ods-explorer.us_re_fm_prod.fsai_workorders` wo
    WHERE (
        wo.trade_name LIKE '%HVAC%'
        OR wo.trade_name LIKE '%Refrig%'
    )
    AND wo.completion_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 18 MONTH)
    ORDER BY wo.completion_date DESC
""", max_rows=300_000)


run_query("observations", """
    SELECT
        QuestionId,
        question,
        answer,
        Question_group,
        User_Id,
        ASC_USERID,
        CONCAT(FIRST_NAME, ' ', LAST_NAME) AS tech_name,
        Org_Role,
        Supervisor_Name,
        Direct_Manager,
        FM_Sr_Director,
        FM_Director,
        FM_Regional_Manager,
        Store,
        Date,
        status,
        wmt_year_nbr,
        wmt_week_of_year_nbr
    FROM `re-ods-explorer.us_re_fm_prod.FSAI_Tech_Survey_Observation`
    ORDER BY Date DESC
""")


run_query("top_gun", """
    SELECT
        Store_nbr,
        Status,
        Call_Date,
        Completion_Date,
        Project_duration_in_days,
        Before_Workorders,
        After_Workorders,
        `%Reduction_in_Workorders`  AS pct_reduction_wo,
        Time_in_Target_Before,
        Time_in_Target_After,
        `%Difference_Time_in_Target` AS pct_diff_tnt,
        Problem_code_desc,
        Region,
        State,
        FM_Sr_Director,
        FM_Director,
        FM_Regional_Mgr
    FROM `re-ods-explorer.us_re_fm_prod.fsai_top_gun_technicians`
    ORDER BY Call_Date DESC
""")


run_query("pm_compliance", """
    SELECT *
    FROM `re-ods-explorer.us_re_fm_prod.fsai_CMMS_PM_Compliance`
    ORDER BY 1
""", max_rows=200_000)


print("\n" + "="*60)
print("  ✅ Training data fetch complete")
print("="*60 + "\n")
