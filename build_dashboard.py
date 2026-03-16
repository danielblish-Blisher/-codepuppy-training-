"""build_dashboard.py — Assemble the FM Training Intelligence dashboard.

Loads JSON data from data/, injects into HTML templates, and writes
the final self-contained file to docs/index.html.

Usage:
    .venv/bin/python build_dashboard.py
"""
import json
from datetime import datetime
from pathlib import Path

ROOT     = Path(__file__).parent
DATA_DIR = ROOT / "data"
TMPL_DIR = ROOT / "templates"
DOCS_DIR = ROOT / "docs"
OUT_FILE = DOCS_DIR / "index.html"

DOCS_DIR.mkdir(exist_ok=True)


def load_json(name: str, default=None) -> list | dict:
    path = DATA_DIR / f"{name}.json"
    if not path.exists():
        print(f"  ⚠️  {path.name} not found, using default")
        return default if default is not None else []
    with open(path) as f:
        return json.load(f)


def tmpl(name: str) -> str:
    return (TMPL_DIR / name).read_text()


def embed(data) -> str:
    """Serialize data to compact JSON for embedding in JS."""
    return json.dumps(data, default=str, separators=(",", ":"))


def build_repeat_wo_data(raw_rows):
    """
    Process raw aggregated repeat rows from BQ (all_fm_wos.json) into the
    compact format expected by repeat_filter_js.html.
    """
    print(f"  Processing {len(raw_rows)} repeat WO groups...")
    out = []
    for r in raw_rows:
        # Source fields from fetch_repeat.py:
        # store_nbr, problem_type_desc, problem_code_desc, sc_trade_name, category_name,
        # fm_director, sr_director, completion_count, first_completion, last_completion,
        # total_nte, avg_nte, min_days_between, repeat_count_30d, tracking_numbers,
        # sample_descs_raw, asset_counts_str

        # Format needed by frontend:
        # {sn, prob, trade, cat, cnt, fc, lc, nte, avgnte, mindays, r30, sr, dir, rm, fsm, mkt, tnums, descs, assets, pcd}
        
        prob_type = r.get("problem_type_desc", "")
        prob_code = r.get("problem_code_desc", "")
        # If problem code exists and is different/more specific, use it or combine?
        # Frontend logic: prob = problem_type_desc usually. 
        # But wait, repeat_filter_js says:
        # "pcdDisplay = w.pcd && w.pcd !== w.prob"
        # So `prob` should be the high-level grouping (Type), and `pcd` the specific Code.
        
        # Actually, let's look at how the BQ query groups:
        # GROUP BY store_nbr, problem_type_desc, problem_code_desc
        # So each row is unique to Type+Code.
        
        # We'll use Type as the main `prob`, and Code as `pcd`.
        
        row = {
            "sn":      r.get("store_nbr"),
            "prob":    prob_type,
            "pcd":     prob_code,
            "trade":   r.get("sc_trade_name"),
            "cat":     r.get("category_name"),
            "cnt":     r.get("completion_count"),
            "fc":      r.get("first_completion"),
            "lc":      r.get("last_completion"),
            "nte":     r.get("total_nte"),
            "avgnte":  r.get("avg_nte"),
            "mindays": r.get("min_days_between"),
            "r30":     r.get("repeat_count_30d"),
            "sr":      r.get("sr_director") or "Unassigned",
            "dir":     r.get("fm_director") or "Unassigned",
            "rm":      r.get("regional_mgr") or "Unassigned",
            "fsm":     r.get("fs_mgr") or "Unassigned",
            "mkt":     "",  # Not fetched
            "tnums":   r.get("tracking_numbers"),
            "descs":   r.get("sample_descs_raw"),
            "assets":  r.get("asset_counts_str"),
        }
        out.append(row)
    return out


def build() -> None:
    print("\n" + "="*60)
    print("🛠️  Building FM Training Intelligence Dashboard")
    print("="*60)

    generated = datetime.now().strftime("%Y-%m-%d %H:%M")

    # ─ Load data ─────────────────────────────────────────────────
    repeats_by_trade = load_json("repeats_by_trade",  [])
    problem_codes    = load_json("problem_codes",      [])
    store_hotspots   = load_json("store_hotspots",     [])
    monthly_trend    = load_json("monthly_trend",      [])
    repeat_detail    = load_json("repeat_detail",      [])
    tech_repeats     = load_json("tech_repeats",       [])
    tech_output      = load_json("tableau_tech_output",[])
    metric_pulse     = load_json("tableau_metric_pulse",[])
    
    # Load and process the new heavy repeat data
    all_fm_wos_raw   = load_json("all_fm_wos",         [])
    repeat_wos_clean = build_repeat_wo_data(all_fm_wos_raw)

    print(f"  repeats_by_trade : {len(repeats_by_trade)} trades")
    print(f"  problem_codes    : {len(problem_codes)} codes")
    print(f"  store_hotspots   : {len(store_hotspots)} stores")
    print(f"  monthly_trend    : {len(monthly_trend)} rows")
    print(f"  repeat_detail    : {len(repeat_detail)} rows")
    print(f"  tech_repeats     : {len(tech_repeats)} rows")
    print(f"  tech_output      : {len(tech_output)} rows")
    print(f"  metric_pulse     : {len(metric_pulse)} rows")
    print(f"  repeat_wos_clean : {len(repeat_wos_clean)} rows")

    # ─ Assemble HTML ───────────────────────────────────────────────
    # Tabs first, then JS blocks, then footer (which closes body/html)
    html = (
        tmpl("head.html")
        + tmpl("header.html")
        + tmpl("overview_tab.html")
        + tmpl("gaps_tab.html")
        + tmpl("trade_tab.html")
        + tmpl("store_tab.html")
        + tmpl("trends_and_tableau_tabs.html")
        + tmpl("repeat_tab.html")  # New tab content
        + tmpl("overview_js.html")
        + tmpl("gaps_js.html")
        + tmpl("trade_js.html")
        + tmpl("store_js.html")
        + tmpl("trends_and_tableau_js.html")
        + tmpl("repeat_filter_js.html") # New JS logic
        + tmpl("repeat_render_js.html") # New JS rendering
        + tmpl("footer.html")
    )

    # ─ Inject data placeholders ────────────────────────────────────────
    replacements = {
        "'__GENERATED__'":  f"'{generated}'",
        "__GENERATED__":     generated,
        "__REPEATS_BY_TRADE__": embed(repeats_by_trade),
        "__PROBLEM_CODES__":    embed(problem_codes),
        "__STORE_HOTSPOTS__":   embed(store_hotspots),
        "__MONTHLY_TREND__":    embed(monthly_trend),
        "__REPEAT_DETAIL__":    embed(repeat_detail),
        "__TECH_REPEATS__":     embed(tech_repeats),
        "__TECH_OUTPUT__":      embed(tech_output),
        "__METRIC_PULSE__":     embed(metric_pulse),
        "__ALL_WOS__":          embed(repeat_wos_clean),
    }
    for placeholder, value in replacements.items():
        html = html.replace(placeholder, value)

        # ─ Write output ─────────────────────────────────────────────────
    OUT_FILE.write_text(html)
    size_kb = OUT_FILE.stat().st_size / 1024
    print(f"\n  ✅ Dashboard written to {OUT_FILE}")
    print(f"     Size: {size_kb:.0f} KB | Generated: {generated}")
    print("="*60)


if __name__ == "__main__":
    build()
