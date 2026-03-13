#!/usr/bin/env python3
"""Dev server with no-cache headers so Safari always fetches fresh builds."""
import http.server, sys

class NoCacheHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate')
        self.send_header('Pragma', 'no-cache')
        self.send_header('Expires', '0')
        super().end_headers()

    def log_message(self, fmt, *args):
        print(fmt % args, flush=True)

import os
os.chdir(os.path.join(os.path.dirname(__file__), 'docs'))
port = int(sys.argv[1]) if len(sys.argv) > 1 else 8766
http.server.test(HandlerClass=NoCacheHandler, port=port, bind='127.0.0.1')