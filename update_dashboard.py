"""Orchestrate full HVACR Training Dashboard update.

Steps:
  1. Fetch training data from BQ + Tableau
  2. Build dashboard HTML from templates
  3. Git commit + push to GitHub Pages
"""
import subprocess
import sys
import time
from datetime import datetime


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def run(label: str, cmd: list[str], cwd: str = ".") -> bool:
    start = time.time()
    print(f"\n{_ts()} | ─ {label}...")
    result = subprocess.run(cmd, cwd=cwd, capture_output=False, text=True)
    elapsed = int(time.time() - start)
    if result.returncode != 0:
        print(f"{_ts()} |   ❌ {label} FAILED ({elapsed}s)")
        return False
    print(f"{_ts()} |   ✅ {label} ({elapsed}s) ── Done ──")
    return True


python = sys.executable

print("\n" + "═"*58)
print("  🍯 HVACR Training Dashboard Update")
print("═"*58)

steps = [
    ("Fetch BQ Training Data",  [python, "fetch_training_data.py"]),
    ("Build Dashboard HTML",     [python, "build_dashboard.py"]),
]

for label, cmd in steps:
    if not run(label, cmd):
        print(f"\n❌ Halted at: {label}")
        sys.exit(1)

# ── Git commit + push ────────────────────────────────────────────────

print(f"\n{_ts()} | ─ Git commit + push...")
tag = datetime.now().strftime("%Y-%m-%d %H:%M")
git_steps = [
    ["git", "add", "docs/", "data/"],
    ["git", "commit", "-m", f"chore: update training dashboard data ({tag})"],
    ["git", "push", "origin", "main"],
]
for gcmd in git_steps:
    result = subprocess.run(gcmd, capture_output=True, text=True)
    if result.returncode != 0 and "nothing to commit" not in result.stdout + result.stderr:
        print(f"  ⚠️  git: {result.stderr.strip() or result.stdout.strip()}")

print("\n" + "═"*58)
print(f"  ✅ Training Dashboard Updated!")
print(f"  Updated: {tag}")
print(f"  Local:   http://localhost:9100/dashboard.html")
print("═"*58 + "\n")
