"""A stand-in for an ntfy server, for tests/notify.sh.

    python3 tests/fake_ntfy.py <port> <file>

Every POST is appended to <file> as a line of JSON: {"path", "headers" (names
in lower case), "body"}. It answers 200 with ntfy's JSON, or 500 while the
file <file>.fail exists. Nothing is ever sent anywhere else.
"""

import http.server
import json
import os
import sys

PORT = int(sys.argv[1])
FILE = sys.argv[2]


class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(n).decode("utf-8", "replace")
        if self.path == "/_up":  # the test's probe for the server being up
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if os.path.exists(FILE + ".fail"):
            self.send_response(500)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        rec = {
            "path": self.path,
            "headers": {k.lower(): v for k, v in self.headers.items()},
            "body": body,
        }
        with open(FILE, "a") as f:
            f.write(json.dumps(rec) + "\n")
        out = b'{"id": "x", "event": "message"}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    def log_message(self, *args):
        pass


http.server.ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
