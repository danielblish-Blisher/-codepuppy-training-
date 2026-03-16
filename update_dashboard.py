"""update_dashboard.py — Orchestrate fetch → build for the training dashboard.

Usage:
    .venv/bin/python update_dashboard.py
    .venv/bin/python update_dashboard.py --skip-bq
    .venv/bin/python update_dashboard.py --skip-tableau
    .venv/bin/python update_dashboard.py --build-only
"""
import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT   = Path(__file__).parent
PY     = ROOT / ".venv" / "bin" / "python"
LOG    = ROOT / "update.log"


def log(msg: str) -> None:
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} | {msg}"
    print(line)
    with open(LOG, "a") as f:
        f.write(line + "\n")


def run_step(name: str, script: str, timeout: int = 600) -> bool:
    log(f"─ {name}...")
    t0 = time.time()
    try:
        r = subprocess.run(
            [str(PY), str(ROOT / script)],
            capture_output=True, text=True,
            timeout=timeout, cwd=str(ROOT),
        )
        elapsed = time.time() - t0
        if r.returncode == 0:
            lines   = [l for l in r.stdout.strip().splitlines() if l.strip()]
            summary = lines[-1][:120] if lines else "done"
            log(f"  ✅ {name} ({elapsed:.0f}s) — {summary}")
            return True
        err = (r.stderr.strip().splitlines() or ["exit " + str(r.returncode)])[-1][:200]
        log(f"  ⚠️  {name} ({elapsed:.0f}s): {err}")
        return False
    except subprocess.TimeoutExpired:
        log(f"  ❌ {name}: timed out after {timeout}s")
        return False
    except Exception as e:
        log(f"  ❌ {name}: {e}")
        return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-bq",      action="store_true")
    ap.add_argument("--skip-tableau", action="store_true")
    ap.add_argument("--build-only",   action="store_true")
    args = ap.parse_args()

    log("=" * 56)
    log("📚 FM Training Dashboard Update Starting")
    log("=" * 56)

    results = []

    if not args.build_only:
        if not args.skip_bq:
            results.append(run_step("BQ Data Fetch", "fetch_data.py", timeout=600))
            results.append(run_step("Repeat WO Fetch", "fetch_repeat.py", timeout=300))
        if not args.skip_tableau:
            results.append(run_step("Tableau Fetch", "fetch_tableau.py", timeout=120))

    results.append(run_step("Build Dashboard", "build_dashboard.py", timeout=60))

    ok    = sum(results)
    total = len(results)
    log("=" * 56)
    log(f"  Update complete: {ok}/{total} steps ok")
    log("=" * 56)

    # Print dashboard path for easy opening
    out = ROOT / "docs" / "index.html"
    if out.exists():
        size_kb = out.stat().st_size / 1024
        log(f"  📤 Dashboard: {out}  ({size_kb:.0f} KB)")

    return 0 if ok == total else 1


if __name__ == "__main__":
    sys.exit(main())
