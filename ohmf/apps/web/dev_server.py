from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import os
import threading
from pathlib import Path
from functools import partial


class NoCacheHandler(SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()


class MiniappSandboxHandler(SimpleHTTPRequestHandler):
    """Serves mini-apps from separate origin with strict CSP"""
    sandbox_dir = "."

    def translate_path(self, path):
        # Always serve from the sandbox_dir (miniapps)
        parts = path.split("/")
        # Remove empty parts and join with sandbox_dir
        clean_parts = [p for p in parts if p]
        full_path = os.path.join(self.sandbox_dir, *clean_parts) if clean_parts else self.sandbox_dir
        # Prevent directory traversal
        if not os.path.abspath(full_path).startswith(os.path.abspath(self.sandbox_dir)):
            full_path = self.sandbox_dir
        return full_path

    def end_headers(self):
        # Strict CSP for mini-app sandbox
        self.send_header("Content-Security-Policy",
            "default-src 'self' 'wasm-unsafe-eval'; "
            "script-src 'self' 'wasm-unsafe-eval'; "
            "style-src 'self'; "
            "img-src 'self' data:; "
            "font-src 'self'; "
            "connect-src 'self' http://localhost:* http://127.0.0.1:* ws://localhost:* ws://127.0.0.1:* https: http:; "
            "object-src 'none'; "
            "base-uri 'none'; "
            "frame-src 'none'; "
            "form-action 'none'"
        )
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()


def main():
    client_port = int(os.environ.get("CLIENT_PORT", "5173"))
    miniapp_port = int(os.environ.get("MINIAPP_SANDBOX_PORT", "5174"))

    original_cwd = os.getcwd()
    miniapps_dir = os.path.join(original_cwd, "miniapps")

    # Main app server - serves from root directory
    print(f"Starting main app server on port {client_port}...")
    main_server = ThreadingHTTPServer(("0.0.0.0", client_port), NoCacheHandler)
    main_thread = threading.Thread(target=main_server.serve_forever, daemon=True)
    main_thread.start()

    # Mini-app sandbox server - serves from miniapps directory
    print(f"Starting mini-app sandbox server on port {miniapp_port}...")

    # Create custom handler class with sandbox_dir set
    def create_sandbox_handler():
        class SandboxHandler(MiniappSandboxHandler):
            sandbox_dir = miniapps_dir
        return SandboxHandler

    miniapp_server = ThreadingHTTPServer(("0.0.0.0", miniapp_port), create_sandbox_handler())
    miniapp_thread = threading.Thread(target=miniapp_server.serve_forever, daemon=True)
    miniapp_thread.start()

    print(f"Main app: http://localhost:{client_port}")
    print(f"Mini-app sandbox: http://localhost:{miniapp_port}")

    # Keep servers running
    try:
        main_thread.join()
    except KeyboardInterrupt:
        print("Shutting down servers...")
        main_server.shutdown()
        miniapp_server.shutdown()


if __name__ == "__main__":
    main()
