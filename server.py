"""Local dev server for HVACR Training Dashboard.

Serves docs/ on port 9100 (different port from HVACR dashboard on 9000).
Use Cmd+Shift+R in browser to trigger live data refresh (keyboard shortcut
caught by the app itself — this server just handles static files).
"""
import http.server
import os
import socketserver
from pathlib import Path

PORT = 9100
DOCS = Path("docs")

if not DOCS.exists():
    print(f"ERROR: {DOCS} not found. Run build_dashboard.py first.")
    raise SystemExit(1)

os.chdir(DOCS)

class Handler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, fmt, *args):  # quiet mode
        pass
    def end_headers(self):
        # Allow cross-origin so CDN scripts load cleanly
        self.send_header("Access-Control-Allow-Origin", "*")
        super().end_headers()

print(f"\n🍯 HVACR Training Dashboard")
print(f"   http://localhost:{PORT}/dashboard.html")
print(f"   Ctrl+C to stop\n")

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n  Server stopped.")
