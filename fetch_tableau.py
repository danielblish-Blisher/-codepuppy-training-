"""fetch_tableau.py — Pull Tech Output + Uptime KPI from Tableau.

Searches tableau-realestate.walmart.com by view name (partial, case-insensitive),
downloads CSV data, saving as JSON to data/.

Outputs:
    data/tableau_tech_output.json
    data/tableau_metric_pulse.json

Requires .env with TABLEAU_PAT_NAME and TABLEAU_PAT_SECRET.
"""
import csv
import io
import json
import sys
import threading
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import tableauserverclient as TSC

SERVER_URL   = "https://tableau-realestate.walmart.com"
API_VERSION  = "3.22"
DATA_DIR     = Path(__file__).parent / "data"
ENV_PATH     = Path(__file__).parent / ".env"
CSV_TIMEOUT  = 45   # seconds before giving up on populate_csv
CSV_TIMEOUT_XL = 120  # for known-large views

DATA_DIR.mkdir(exist_ok=True)


def _load_env() -> tuple[str, str]:
    if not ENV_PATH.exists():
        print("  ❌ .env not found — add TABLEAU_PAT_NAME and TABLEAU_PAT_SECRET")
        sys.exit(1)
    creds: dict[str, str] = {}
    for line in ENV_PATH.read_text().splitlines():
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, _, v = line.partition("=")
            creds[k.strip()] = v.strip()
    return creds["TABLEAU_PAT_NAME"], creds["TABLEAU_PAT_SECRET"]


def _connect() -> TSC.Server:
    pat_name, pat_secret = _load_env()
    server = TSC.Server(SERVER_URL, use_server_version=False)
    server.version = API_VERSION
    server._session.verify = False  # corp SSL — self-signed cert
    auth = TSC.PersonalAccessTokenAuth(pat_name, pat_secret, site_id="")
    server.auth.sign_in(auth)
    return server


def _csv_to_rows(raw_csv: bytes) -> list[dict]:
    text   = raw_csv.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


def _find_view(server: TSC.Server, view_name: str) -> "TSC.ViewItem | None":
    """Search all accessible views for one whose name contains view_name."""
    vw_lower = view_name.lower()
    req = TSC.RequestOptions(pagesize=1000)
    all_views, _ = server.views.get(req)
    for view in all_views:
        if vw_lower in view.name.lower():
            return view
    return None


def _populate_with_timeout(server: TSC.Server,
                           view: "TSC.ViewItem",
                           timeout: int = CSV_TIMEOUT) -> bytes:
    """Run populate_csv in a daemon thread; raise TimeoutError if too slow."""
    bucket: dict = {"data": None, "err": None}

    def _worker():
        try:
            server.views.populate_csv(view)
            bucket["data"] = b"".join(view.csv)
        except Exception as exc:
            bucket["err"] = exc

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        raise TimeoutError(f"populate_csv timed out after {timeout}s")
    if bucket["err"]:
        raise bucket["err"]
    return bucket["data"]


def _save_empty(out_name: str) -> None:
    with open(DATA_DIR / f"{out_name}.json", "w") as f:
        json.dump([], f)


def fetch_view(server: TSC.Server, view_name: str,
               out_name: str, timeout: int = CSV_TIMEOUT) -> bool:
    """Find a view by partial name, download CSV, write JSON."""
    print(f"  → searching for '{view_name}'...")
    try:
        view = _find_view(server, view_name)
        if not view:
            print(f"  ⚠️  View not found: '{view_name}'")
            _save_empty(out_name)
            return False

        print(f"  ✓  Found: {view.name!r} — downloading CSV (up to {timeout}s)...")
        raw  = _populate_with_timeout(server, view, timeout=timeout)
        rows = _csv_to_rows(raw)
        with open(DATA_DIR / f"{out_name}.json", "w") as f:
            json.dump(rows, f, indent=2)
        print(f"  ✅ {out_name}: {len(rows)} rows")
        return True

    except Exception as exc:
        print(f"  ❌ {out_name}: {exc}")
        _save_empty(out_name)
        return False


def main() -> int:
    print("\n" + "=" * 60)
    print("📊 Tableau Fetch — Tech Output + Uptime KPI")
    print("=" * 60)
    try:
        server = _connect()
        print("  ✅ Authenticated to Tableau")
    except Exception as exc:
        print(f"  ❌ Auth failed: {exc}")
        _save_empty("tableau_tech_output")
        _save_empty("tableau_metric_pulse")
        return 1

    # "Tech Output" view is massive. Switched to "Technician Summary" (aggregated)
    ok += fetch_view(server, "Technician Summary",   "tableau_tech_output",  timeout=CSV_TIMEOUT_XL)
    ok += fetch_view(server, "Uptime KPI Dashboard", "tableau_metric_pulse")

    try:
        server.auth.sign_out()
    except Exception:
        pass

    print(f"\n  Done: {ok}/2 Tableau views fetched")
    return 0 if ok == 2 else 1


if __name__ == "__main__":
    sys.exit(main())
