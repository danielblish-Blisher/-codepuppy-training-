"""Build Training Dashboard.

Loads JSON data files, enriches WO records with org hierarchy,
then stitches templates into docs/dashboard.html.
"""
import json
from datetime import datetime
from pathlib import Path

TEMPLATES = Path("templates")
DOCS      = Path("docs")
DATA      = Path("data")
DOCS.mkdir(exist_ok=True)


# ── Helpers ──────────────────────────────────────────────────

def load(name: str, default=None):
    path = DATA / f"{name}.json"
    if not path.exists():
        print(f"  ⚠️  {path} not found, using default")
        return default if default is not None else []
    with open(path) as f:
        return json.load(f)


def tpl(name: str) -> str:
    path = TEMPLATES / name
    if not path.exists():
        print(f"  ⚠️  template {path} not found")
        return ""
    return path.read_text()


def js_var(name: str, data) -> str:
    """Emit a compact JS const declaration."""
    return f"const {name} = {json.dumps(data, default=str, separators=(',',':'))};\n"


# ── Load raw data ────────────────────────────────────────────────

print("  Loading data files…")
exam_reg             = load("exam_reg")
tech_align           = load("tech_alignment")
tech_stores          = load("tech_stores")
training_wo          = load("training_workorders")
observations         = load("observations")
top_gun              = load("top_gun")
pm_comply            = load("pm_compliance")
epa_certs_raw        = load("epa_certs")
refrig_training_raw  = load("refrig_training_stores")


def _f(v):
    """Coerce to float, None on failure."""
    try:
        return float(v) if v not in (None, "") else None
    except (ValueError, TypeError):
        return None


# ── Build store-to-org mapping from tech_stores ──────────────────────
# Each row: user_id, store_nbr, rm, director, sr_director, store_type

print("  Building store → org hierarchy…")
store_org: dict[str, dict] = {}
for row in tech_stores:
    sn = str(row.get("store_nbr") or "").strip()
    if not sn or sn in store_org:
        continue
    store_org[sn] = {
        "rm":  row.get("rm") or "",
        "dir": row.get("director") or "",
        "sr":  row.get("sr_director") or "",
        "st":  row.get("store_type") or "",
    }


# ── Pre-aggregate WO data (don't embed 298K raw rows!) ─────────────────
#
# Produce two compact structures:
#   STORE_WO_METRICS  - one row per store with WO stats
#   PROB_METRICS      - one row per problem code with global stats
#

print("  Aggregating work order data…")

store_agg: dict = {}
prob_agg:  dict = {}

for w in training_wo:
    sn    = str(w.get("store_nbr") or "").strip()
    prob  = (w.get("problem_code_desc") or "Unknown").strip() or "Unknown"
    fcr_v = (w.get("first_time_fix_compliance") or "").strip().upper()
    sla_v = (w.get("sla_repair_compliance")     or "").strip().upper()
    trips = _f(w.get("trip_count")) or 0.0

    # Store aggregation
    if sn not in store_agg:
        store_agg[sn] = {
            "store_nbr":   sn,
            "sr_director": w.get("fm_sr_director")  or "",
            "director":    w.get("fm_director")      or "",
            "rm":          w.get("fm_regional_mgr") or "",
            "store_type":  w.get("store_type_name") or "",
            "total_wos":   0,
            "fcr_y": 0, "fcr_n": 0,
            "sla_y": 0, "sla_n": 0,
            "trip_sum": 0.0, "trip_n": 0,
            "prob_counts": {},
        }
    s = store_agg[sn]
    s["total_wos"] += 1
    # BQ values: "Yes First Time Fix" / "No First Time Fix" / "Missing Time"
    if fcr_v.startswith("YES"): s["fcr_y"] += 1
    elif fcr_v.startswith("NO"):  s["fcr_n"] += 1
    # BQ values: "Yes" / "No" / ""
    if sla_v.startswith("YES"): s["sla_y"] += 1
    elif sla_v.startswith("NO"):  s["sla_n"] += 1
    if trips > 0:
        s["trip_sum"] += trips
        s["trip_n"]   += 1
    s["prob_counts"][prob] = s["prob_counts"].get(prob, 0) + 1

    # Global problem code aggregation
    if prob not in prob_agg:
        prob_agg[prob] = {"prob": prob, "total": 0, "fcr_y": 0, "fcr_n": 0, "repeat_wos": 0}
    p = prob_agg[prob]
    p["total"] += 1
    if fcr_v.startswith("YES"): p["fcr_y"] += 1
    elif fcr_v.startswith("NO"):  p["fcr_n"] += 1

# Compute per-store repeat WOs & finalise
for s in store_agg.values():
    repeat_wos = sum(n for n in s["prob_counts"].values() if n > 1)
    top_entries = sorted(s["prob_counts"].items(), key=lambda x: -x[1])
    s["repeat_wos"]  = repeat_wos
    s["top_prob"]    = top_entries[0][0] if top_entries else ""
    s["avg_trips"]   = round(s["trip_sum"] / s["trip_n"], 2) if s["trip_n"] else None
    del s["prob_counts"], s["trip_sum"], s["trip_n"]  # don't embed prob_counts raw

# Compact problem-code list (top 100 by volume)
prob_list = sorted(prob_agg.values(), key=lambda x: -x["total"])[:100]
# Compute repeat count from store_agg
for p in prob_list:
    # Already summed in loop above; compute cross-store repeat signal
    p["fcr_rate"] = round(p["fcr_y"] / (p["fcr_y"]+p["fcr_n"]), 3) \
        if (p["fcr_y"]+p["fcr_n"]) > 0 else None

store_wo_list = list(store_agg.values())

print(f"  Store WO metrics: {len(store_wo_list):,} stores")
print(f"  Problem codes:    {len(prob_list):,}")



# ── Compact exam reg ────────────────────────────────────────────────
# Keep field names matching what technicians_js.html expects

def _clean_score(v):
    try:
        return float(v) if v not in (None, "") else None
    except (ValueError, TypeError):
        return None

compact_exam = [{
    "student_id":    str(e.get("student_id") or ""),
    "student_name":  e.get("student_name") or "",
    "student_email": e.get("student_email") or "",
    "test_type":     e.get("test_type") or "",
    "Testing_Date":  str(e.get("Testing_Date") or "")[:10],
    "Score":         _clean_score(e.get("Score")),
    "Current_Tech_Level": e.get("Current_Tech_Level") or "",
    "attempt":       str(e.get("attempt") or ""),
    "Result":        e.get("Result") or "",
    "wmt_year_nbr":  int(e["wmt_year_nbr"]) if e.get("wmt_year_nbr") else None,
    "wmt_week_of_year_nbr": int(e["wmt_week_of_year_nbr"]) if e.get("wmt_week_of_year_nbr") else None,
} for e in exam_reg]


# ── Compact tech alignment ────────────────────────────────────────────

compact_tech = [{
    "user_id":           str(t.get("user_id") or ""),
    "first_name":        t.get("first_name") or "",
    "last_name":         t.get("last_name") or "",
    "status":            t.get("status") or "",
    "org_role":          t.get("org_role") or "",
    "technician_level":  t.get("technician_level") or "",
    "home_store":        str(t.get("home_store") or ""),
    "mgr_first_name":    t.get("mgr_first_name") or "",
    "mgr_last_name":     t.get("mgr_last_name") or "",
    "mgr_org_role":      t.get("mgr_org_role") or "",
    "mgr_technician_level": t.get("mgr_technician_level") or "",
} for t in tech_align]


# ── Compact tech-stores — include org hierarchy fields for slicer ───────
# Also build ORG_BY_UID: user_id → {sr, dir, rm} for JS filter cascade.

org_by_uid: dict = {}
for ts in tech_stores:
    uid = str(ts.get("user_id") or "").strip()
    if uid and uid not in org_by_uid:
        org_by_uid[uid] = {
            "sr":  (ts.get("sr_director") or "").strip(),
            "dir": (ts.get("director")    or "").strip(),
            "rm":  (ts.get("rm")          or "").strip(),
        }

compact_tech_stores = [
    {"user_id": str(ts.get("user_id") or ""), "store_nbr": str(ts.get("store_nbr") or "")}
    for ts in tech_stores
    if ts.get("user_id") and ts.get("store_nbr")
]


# ── Pre-aggregate observations (don't embed 39K raw rows) ─────────────────
#
# Produce compact structures the JS can use directly:
#   OBS_STATS      - KPI totals
#   OBS_GROUPS     - [{group, count}] sorted by count
#   OBS_TREND      - [{key, count}] weekly trend (last 26 weeks)
#   OBS_SR_LIST    - sorted list of Sr Directors (for filter)
#   OBS_GROUP_LIST - sorted list of Question Groups (for filter)
#   OBS_ROWS       - most recent 500 rows for the detail table
#

obs_group_counts: dict = {}
obs_week_counts:  dict = {}
obs_sr_set:       set  = set()
obs_group_set:    set  = set()
obs_uid_set:      set  = set()
obs_store_set:    set  = set()

for o in observations:
    grp  = o.get("Question_group") or "Unknown"
    yr   = o.get("wmt_year_nbr")
    wk   = o.get("wmt_week_of_year_nbr")
    sr   = o.get("FM_Sr_Director") or ""
    uid  = o.get("User_Id") or ""
    sn   = str(o.get("Store") or "")

    obs_group_counts[grp]  = obs_group_counts.get(grp, 0) + 1
    obs_group_set.add(grp)
    if sr:  obs_sr_set.add(sr)
    if uid: obs_uid_set.add(uid)
    if sn:  obs_store_set.add(sn)
    if yr and wk:
        key = f"FY{yr}-W{str(wk).zfill(2)}"
        obs_week_counts[key] = obs_week_counts.get(key, 0) + 1

# Trend: sort all weeks, keep last 26
all_weeks = sorted(obs_week_counts.keys())
trend_weeks = all_weeks[-26:]
obs_trend = [{"key": k, "count": obs_week_counts[k]} for k in trend_weeks]

# Groups sorted by count desc
obs_groups = [
    {"group": g, "count": c}
    for g, c in sorted(obs_group_counts.items(), key=lambda x: -x[1])
]

obs_stats = {
    "total":   len(observations),
    "techs":   len(obs_uid_set),
    "stores":  len(obs_store_set),
    "groups":  len(obs_group_set),
}

# Keep 2000 most recent rows for the detail table (Date desc)
observations_sorted = sorted(
    observations,
    key=lambda o: str(o.get("Date") or ""),
    reverse=True,
)
OBS_COLS = (
    "Question_group", "question", "answer",
    "User_Id", "tech_name", "Org_Role",
    "Direct_Manager", "FM_Sr_Director", "FM_Director", "FM_Regional_Manager",
    "Store", "Date", "status",
)
compact_obs_rows = [
    {k: str(o.get(k) or "")[:100] for k in OBS_COLS}
    for o in observations_sorted[:2000]
]


# ── Refrigerant materials (pre-aggregate from sc_walmart_materials JOIN) ────

refrig_raw = load("refrigerant_materials", default=[])

_refrig_type:   dict = {}   # type_name -> {events, lbs}
_refrig_reason: dict = {}   # reason    -> {events, lbs}
_refrig_month:  dict = {}   # YYYY-MM   -> {events, lbs}
_refrig_sr:     dict = {}   # sr_dir    -> {events, lbs}
_refrig_stores: set  = set()

for r in refrig_raw:
    rtype  = (r.get("refrigerant_type_name") or "Unknown").strip()
    reason = (r.get("refrigerant_reason")    or "Unknown").strip() or "Unknown"
    sr     = (r.get("fm_sr_director")        or "").strip()
    sn     = str(r.get("store_nbr")          or "").strip()
    lbs    = float(r.get("lbs_used") or 0)
    dt     = str(r.get("use_date") or "")[:7]  # YYYY-MM

    if sn:
        _refrig_stores.add(sn)

    for bucket, key in [(_refrig_type, rtype), (_refrig_reason, reason),
                         (_refrig_month, dt),   (_refrig_sr, sr)]:
        if not key:
            continue
        if key not in bucket:
            bucket[key] = {"events": 0, "lbs": 0.0}
        bucket[key]["events"] += 1
        bucket[key]["lbs"]    += lbs

refrig_stats = {
    "total_events": len(refrig_raw),
    "total_lbs":    round(sum(float(r.get("lbs_used") or 0) for r in refrig_raw), 1),
    "stores":       len(_refrig_stores),
    "types":        len(_refrig_type),
}

refrig_by_type = sorted(
    [{"type": k, "events": v["events"], "lbs": round(v["lbs"], 1)}
     for k, v in _refrig_type.items()],
    key=lambda x: x["lbs"], reverse=True
)

refrig_by_reason = sorted(
    [{"reason": k, "events": v["events"], "lbs": round(v["lbs"], 1)}
     for k, v in _refrig_reason.items()],
    key=lambda x: x["events"], reverse=True
)

refrig_trend = [
    {"month": k, "events": v["events"], "lbs": round(v["lbs"], 1)}
    for k, v in sorted(_refrig_month.items())
]

refrig_by_sr = sorted(
    [{"sr": k, "events": v["events"], "lbs": round(v["lbs"], 1)}
     for k, v in _refrig_sr.items() if k],
    key=lambda x: x["lbs"], reverse=True
)[:25]

# 500 most-recent rows for the detail table
_REFRIG_COLS = (
    "tracking_nbr", "refrigerant_type_name", "lbs_used",
    "refrigerant_reason", "use_date", "is_ods",
    "store_nbr", "fm_sr_director", "fm_director", "trade_name",
)
refrig_recent = [
    {k: str(r.get(k) or "")[:80] for k in _REFRIG_COLS}
    for r in refrig_raw[:500]
]





# ── Top Gun (pass-through, already clean) ───────────────────────────

compact_tg = [{
    "Store_nbr":                str(d.get("Store_nbr") or ""),
    "Status":                   d.get("Status") or "",
    "Call_Date":                str(d.get("Call_Date") or "")[:10],
    "Completion_Date":          str(d.get("Completion_Date") or "")[:10],
    "Project_duration_in_days": _f(d.get("Project_duration_in_days")),
    "Before_Workorders":        _f(d.get("Before_Workorders")),
    "After_Workorders":         _f(d.get("After_Workorders")),
    "pct_reduction_wo":         _f(d.get("pct_reduction_wo")),
    "Time_in_Target_Before":    _f(d.get("Time_in_Target_Before")),
    "Time_in_Target_After":     _f(d.get("Time_in_Target_After")),
    "pct_diff_tnt":             _f(d.get("pct_diff_tnt")),
    "Problem_code_desc":        d.get("Problem_code_desc") or "",
    "Region":                   d.get("Region") or "",
    "State":                    d.get("State") or "",
    "FM_Sr_Director":           d.get("FM_Sr_Director") or "",
    "FM_Director":              d.get("FM_Director") or "",
    "FM_Regional_Mgr":          d.get("FM_Regional_Mgr") or "",
} for d in top_gun]


# ── Summary stats ─────────────────────────────────────────────────────

updated = datetime.now().strftime("%Y-%m-%d %H:%M")
print(f"  Total WOs:     {len(training_wo):,} (raw, not embedded)")
print(f"  Exam records:  {len(compact_exam):,}")
print(f"  Technicians:   {len(compact_tech):,}")
print(f"  Tech-stores:   {len(compact_tech_stores):,} (compact pairs)")
print(f"  Observations:  {obs_stats['total']:,} \u2192 {len(compact_obs_rows)} table rows")
print(f"  Refrigerant:   {refrig_stats['total_events']:,} events, {refrig_stats['total_lbs']:,.0f} lbs")
print(f"  Top Gun:       {len(compact_tg):,}")


# ── Build data block ─────────────────────────────────────────────────────

data_block = "<script>\n"
data_block += js_var("DASHBOARD_UPDATED", updated)
# Pre-aggregated WO metrics (no raw row embed)
data_block += js_var("STORE_WO_METRICS", store_wo_list)
data_block += js_var("PROB_METRICS",     prob_list)
# Exam records
data_block += js_var("EXAM_REG",         compact_exam)
# Technician roster
data_block += js_var("TECH_ALIGNMENT",   compact_tech)
# Tech-to-store pairs (compact: user_id + store_nbr only)
data_block += js_var("TECH_STORES",      compact_tech_stores)
data_block += js_var("ORG_BY_UID",        org_by_uid)
# Observations - pre-aggregated + 2000 recent rows
data_block += js_var("OBS_STATS",        obs_stats)
data_block += js_var("OBS_GROUPS",       obs_groups)
data_block += js_var("OBS_TREND",        obs_trend)
data_block += js_var("OBS_SR_LIST",      sorted(obs_sr_set))
data_block += js_var("OBS_GROUP_LIST",   sorted(obs_group_set))
data_block += js_var("OBS_ROWS",         compact_obs_rows)
# Refrigerant materials
data_block += js_var("REFRIG_STATS",      refrig_stats)
data_block += js_var("REFRIG_BY_TYPE",    refrig_by_type)
data_block += js_var("REFRIG_BY_REASON",  refrig_by_reason)
data_block += js_var("REFRIG_TREND",      refrig_trend)
data_block += js_var("REFRIG_BY_SR",          refrig_by_sr)
data_block += js_var("REFRIG_RECENT",         refrig_recent)
# EPA certs (was missing — fix!)
data_block += js_var("EPA_CERTS",             epa_certs_raw)
# Refrigeration training opportunity store rankings
data_block += js_var("REFRIG_TRAINING_STORES", refrig_training_raw)
data_block += js_var("TOP_GUN",               compact_tg)
data_block += "</script>\n"


# ── Stitch template ─────────────────────────────────────────────────────

TEMPLATE_ORDER = [
    "head.html",
    # data injected here
    "header.html",
    "filter_bar.html",
    # tab layouts
    "overview_tab.html",
    "technicians_tab.html",
    "workorders_tab.html",
    "observations_tab.html",
    "refrig_overview_tab.html",
    "refrigerant_tab.html",
    "topgun_tab.html",
    "epa_tab.html",
    # tab JS
    "overview_js.html",
    "technicians_js.html",
    "workorders_js.html",
    "observations_js.html",
    "refrig_overview_js.html",
    "refrigerant_js.html",
    "topgun_js.html",
    "epa_js.html",
    "filters_shared_js.html",
    # closing
    "footer.html",
]

# ── Inline Chart.js (no CDN dependency on corporate network) ────────────
chartjs_path = Path("static") / "chart.umd.min.js"
if chartjs_path.exists():
    chartjs_inline = f"<script>\n{chartjs_path.read_text()}\n</script>"
else:
    print("  ⚠️  static/chart.umd.min.js not found – charts will be blank!")
    chartjs_inline = ""

print("  Stitching templates…")
parts = []
for name in TEMPLATE_ORDER:
    if name == "head.html":
        head_html = tpl(name).replace(
            "<!-- Chart.js inlined at build time by build_dashboard.py -->",
            chartjs_inline,
        )
        parts.append(head_html)
        parts.append(data_block)  # inject data right after <head>
    else:
        parts.append(tpl(name))

out = DOCS / "dashboard.html"
out.write_text("\n".join(parts), encoding="utf-8")
size_mb = out.stat().st_size / 1_000_000
print(f"  ✅ Written: {out} ({size_mb:.1f} MB)")
