# HVACR Training Dashboard

Identifies **where training is needed** across HVACR field service operations
by combining real exam/certification data with work order performance signals.

## Quick Start

```bash
# 1. Fetch fresh data from BigQuery
python3 fetch_training_data.py

# 2. Build the dashboard HTML
python3 build_dashboard.py

# 3. Serve locally (port 9100)
python3 server.py
# → http://localhost:9100/dashboard.html

# Or run everything at once:
python3 update_dashboard.py
```

## Dashboard Tabs

| Tab | What it shows |
|-----|---------------|
| **Overview** | Training Gap Score by store/org, exam pass trend, score distribution |
| **Technicians** | Tech roster, exam scores, level distribution, high-risk list |
| **WO Performance** | FCR, SLA compliance, repeat WOs, avg trips by store/org |
| **Field Observations** | FSAI coaching survey responses by tech/manager/group |
| **EPA Certs** | EPA 608 cert compliance, expiry tracking, license type breakdown |
| **Top Gun** | Before/after WO + TnT metrics from Top Gun interventions |

## Training Gap Score (0–100)

Composite score per store — **higher = more training attention needed**:

| Component | Weight | Source |
|-----------|--------|--------|
| FCR failure rate | 40% | `fsai_workorders.first_time_fix_compliance` |
| SLA miss rate | 20% | `fsai_workorders.sla_repair_compliance` |
| Repeat WO rate | 20% | Same problem code appears >1x at same store |
| Avg trip excess | 10% | Trips per WO above 1.0 (scaled 0–1) |
| EPA non-compliance | 10% | Techs at store without valid EPA cert |

## Data Sources (BigQuery)

| File | BQ Table | Rows (approx) |
|------|----------|--------------|
| `exam_reg.json` | `us_re_fm_prod.doc_exam_reg` | All exam records |
| `epa_certs.json` | `us_re_fm_prod.epa_cert_audit_reports_full` | All active techs |
| `tech_alignment.json` | `us_re_fm_prod.fsai_tech_alignment` | Active techs |
| `tech_stores.json` | `fsai_tech_alignment` (unnested) | Tech → store mapping |
| `training_workorders.json` | `us_re_fm_prod.fsai_workorders` | HVACR WOs (18mo) |
| `observations.json` | `us_re_fm_prod.FSAI_Tech_Survey_Observation` | Field observations |
| `top_gun.json` | `us_re_fm_prod.fsai_top_gun_technicians` | Top Gun outcomes |
| `pm_compliance.json` | `us_re_fm_prod.fsai_CMMS_PM_Compliance` | PM compliance |

All queries billed to `re-ods-explorer`.

## Tech Stack

- **HTML + Tailwind CSS + Chart.js** — single-file self-contained dashboard
- **Python build pipeline** — fetch → build → serve
- **No external dependencies** beyond `bq` CLI (gcloud SDK)
- **Port 9100** (HVACR main dashboard uses 9000 — no conflict)

## Key Commands

```bash
python3 fetch_training_data.py  # Fetch all BQ data
python3 build_dashboard.py      # Build docs/dashboard.html
python3 server.py               # Serve on port 9100
python3 update_dashboard.py     # Full pipeline + git push
```

## Files

```
fetch_training_data.py  ─ BQ queries for all training data
build_dashboard.py      ─ Build HTML from templates + data
server.py               ─ Local dev server (port 9100)
update_dashboard.py     ─ Full pipeline orchestrator
data/                   ─ JSON data files (gitignored)
docs/                   ─ Built dashboard HTML (GitHub Pages)
templates/              ─ HTML template partials
```

## Notes

- Never kill port 8080 (Microsoft Teams)
- Never force-push to git
- Billing project: `re-ods-explorer`
- Related: HVACR Win-the-Winter dashboard at `../hvac-wtw-report/`
