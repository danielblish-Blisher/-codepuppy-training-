"""Build HVACR Training Dashboard.

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
exam_reg    = load("exam_reg")
epa_certs   = load("epa_certs")
tech_align  = load("tech_alignment")
tech_stores = load("tech_stores")
training_wo = load("training_workorders")
observations= load("observations")
top_gun     = load("top_gun")
pm_comply   = load("pm_compliance")


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


# ── Enrich WOs with org hierarchy + normalise types ──────────────────

print("  Enriching work orders…")

def _f(v):
    """Coerce to float, None on failure."""
    try:
        return float(v) if v not in (None, "") else None
    except (ValueError, TypeError):
        return None

compact_wos = []
for w in training_wo:
    sn  = str(w.get("store_nbr") or "").strip()
    org = store_org.get(sn, {})
    compact_wos.append({
        "sn":  sn,
        "tr":  w.get("trade") or "",
        "prob":w.get("problem_code_desc") or "",
        "st":  w.get("status_name") or "",
        "od":  str(w.get("open_date") or "")[:10],
        "nte": _f(w.get("not_to_exceed_amt")),
        "trip":_f(w.get("trip_count")),
        "fcr": (w.get("first_time_fix_compliance") or "").upper() or None,
        "sla": (w.get("sla_repair_compliance")     or "").upper() or None,
        "tech":w.get("orig_hvacr_tech") or "",
        "rm":  org.get("rm",  ""),
        "dir": org.get("dir", ""),
        "sr":  org.get("sr",  ""),
        "store_type": org.get("st", ""),
        # Keep these for filtering compatibility with overview_js
        "sr_director": org.get("sr", ""),
        "director":    org.get("dir", ""),
    })


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


# ── Compact tech-stores (keep only what JS needs) ────────────────────

compact_tech_stores = [{
    "user_id":     str(ts.get("user_id") or ""),
    "tech_name":   ts.get("tech_name") or "",
    "store_nbr":   str(ts.get("store_nbr") or ""),
    "store_type":  ts.get("store_type") or "",
    "rm":          ts.get("rm") or "",
    "director":    ts.get("director") or "",
    "sr_director": ts.get("sr_director") or "",
} for ts in tech_stores]


# ── Compact observations ─────────────────────────────────────────────

compact_obs = [{
    "Question_group":    o.get("Question_group") or "",
    "question":          o.get("question") or "",
    "answer":            o.get("answer") or "",
    "User_Id":           str(o.get("User_Id") or ""),
    "tech_name":         o.get("tech_name") or "",
    "Org_Role":          o.get("Org_Role") or "",
    "Supervisor_Name":   o.get("Supervisor_Name") or "",
    "Direct_Manager":    o.get("Direct_Manager") or "",
    "FM_Sr_Director":    o.get("FM_Sr_Director") or "",
    "FM_Director":       o.get("FM_Director") or "",
    "FM_Regional_Manager": o.get("FM_Regional_Manager") or "",
    "Store":             str(o.get("Store") or ""),
    "Date":              str(o.get("Date") or "")[:10],
    "status":            o.get("status") or "",
    "wmt_year_nbr":      int(o["wmt_year_nbr"]) if o.get("wmt_year_nbr") else None,
    "wmt_week_of_year_nbr": int(o["wmt_week_of_year_nbr"]) if o.get("wmt_week_of_year_nbr") else None,
} for o in observations]


# ── EPA certs (pass-through, already clean) ─────────────────────────

compact_epa = epa_certs


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
print(f"  Training WOs:  {len(compact_wos):,}")
print(f"  Exam records:  {len(compact_exam):,}")
print(f"  Technicians:   {len(compact_tech):,}")
print(f"  Tech-stores:   {len(compact_tech_stores):,}")
print(f"  Observations:  {len(compact_obs):,}")
print(f"  EPA certs:     {len(compact_epa):,}")
print(f"  Top Gun:       {len(compact_tg):,}")


# ── Build data block ─────────────────────────────────────────────────────

data_block = "<script>\n"
data_block += js_var("DASHBOARD_UPDATED", updated)
data_block += js_var("TRAINING_WOS",  compact_wos)
data_block += js_var("EXAM_REG",       compact_exam)
data_block += js_var("TECH_ALIGNMENT", compact_tech)
data_block += js_var("TECH_STORES",    compact_tech_stores)
data_block += js_var("OBSERVATIONS",   compact_obs)
data_block += js_var("EPA_CERTS",      compact_epa)
data_block += js_var("TOP_GUN",        compact_tg)
data_block += "</script>\n"


# ── Stitch template ─────────────────────────────────────────────────────

TEMPLATE_ORDER = [
    "head.html",
    # data injected here
    "header.html",
    # tab layouts
    "overview_tab.html",
    "technicians_tab.html",
    "workorders_tab.html",
    "observations_tab.html",
    "epa_tab.html",
    "topgun_tab.html",
    # tab JS
    "overview_js.html",
    "technicians_js.html",
    "workorders_js.html",
    "observations_js.html",
    "epa_js.html",
    "topgun_js.html",
    # closing
    "footer.html",
]

print("  Stitching templates…")
parts = []
for name in TEMPLATE_ORDER:
    if name == "head.html":
        parts.append(tpl(name))
        parts.append(data_block)  # inject data right after <head>
    else:
        parts.append(tpl(name))

out = DOCS / "dashboard.html"
out.write_text("\n".join(parts), encoding="utf-8")
size_mb = out.stat().st_size / 1_000_000
print(f"  ✅ Written: {out} ({size_mb:.1f} MB)")
