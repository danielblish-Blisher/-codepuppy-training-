"""serve.py — Serve docs/ locally on port 9001 (avoids conflict with main dashboard on 9000)."""
import http.server
import logging
import socketserver
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

ROOT = Path(__file__).parent
DOCS = ROOT / "docs"
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 9001

_handler = RotatingFileHandler(ROOT / "server.log", maxBytes=5*1024*1024, backupCount=2)
_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger = logging.getLogger("training-server")
logger.setLevel(logging.INFO)
logger.addHandler(_handler)


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=str(DOCS), **kw)

    def log_message(self, fmt, *args):
        if args and str(args[1]).startswith(("4", "5")):
            logger.warning(fmt, *args)


socketserver.TCPServer.allow_reuse_address = True
with socketserver.TCPServer(("", PORT), Handler) as srv:
    msg = f"📚 Training Dashboard → http://localhost:{PORT}"
    print(msg)
    logger.info(msg)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n👋 Stopped.")
