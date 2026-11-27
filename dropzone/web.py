"""Local web UI: one static page + a WebSocket for live events + an upload endpoint.

The browser talks only to its *own* node (127.0.0.1 by default). The node then
does the real networking: UDP to discover peers, TCP to move the file.
"""
import asyncio
import json
import mimetypes
import os
import tempfile
from pathlib import Path
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .protocol import ProtocolError, sanitize_filename

STATIC = Path(__file__).resolve().parent.parent / "static"


def _same_origin(headers) -> bool:
    # Stops another website open in your browser from driving this UI
    # (e.g. auto-accepting offers through a cross-site WebSocket).
    origin = headers.get("origin")
    return origin is None or urlparse(origin).netloc == headers.get("host")


def create_app(node) -> FastAPI:
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    app.mount("/static", StaticFiles(directory=STATIC), name="static")

    @app.middleware("http")
    async def no_stale_ui(request, call_next):
        response = await call_next(request)
        if request.url.path == "/" or request.url.path.startswith("/static/"):
            # Make the browser re-check the UI files every time (cheap: unchanged files get a
            # 304 via ETag), so an updated page never runs with an old cached stylesheet.
            response.headers["Cache-Control"] = "no-cache"
        return response

    @app.get("/")
    async def index():
        return FileResponse(STATIC / "index.html")

    @app.post("/api/send")
    async def send(request: Request, peer: str, name: str):
        """Browser streams the file here; we spool it to a temp file, then send it over TCP."""
        if not _same_origin(request.headers):
            raise HTTPException(403, "bad origin")
        if not node.online:
            raise HTTPException(409, "this host is switched off")
        target = node.discovery.peers.get(peer)
        if not target or not target["online"]:
            raise HTTPException(404, "peer is not online")
        try:
            name = sanitize_filename(name)
        except ProtocolError:
            raise HTTPException(400, "bad filename") from None
        fd, tmp = tempfile.mkstemp(dir=node.outbox)
        size = 0
        try:
            with os.fdopen(fd, "wb") as f:
                async for chunk in request.stream():  # streamed, never fully in memory
                    size += len(chunk)
                    if size > node.cfg.max_size:
                        raise HTTPException(413, "file too large")
                    f.write(chunk)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise
        return {"ok": True, "id": node.send_to_peer(target, Path(tmp), name)}

    @app.get("/api/files/{name}")
    async def received_file(name: str):
        """Serve a finished received file for the preview (never .part, never outside the folder)."""
        path = node.received_path(name)
        if path is None:
            raise HTTPException(404, "no such file")
        media = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        # A received .html/.svg must not run scripts as this page (it could click "Accept").
        # CSP "sandbox" gives it an isolated origin; PDFs are exempt so the browser can show them.
        headers = {"X-Content-Type-Options": "nosniff", "Cache-Control": "no-store"}
        if media != "application/pdf":
            headers["Content-Security-Policy"] = "sandbox"
        return FileResponse(path, media_type=media, headers=headers)

    @app.websocket("/ws")
    async def ws(sock: WebSocket):
        if not _same_origin(sock.headers):
            await sock.close(code=1008)
            return
        await sock.accept()
        queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        node.clients.add(queue)

        async def pump():  # node events -> browser
            await sock.send_json({"kind": "snapshot", "data": node.snapshot()})
            while True:
                await sock.send_json(await queue.get())

        pump_task = asyncio.create_task(pump())
        try:
            while True:  # browser -> node: Accept/Reject clicks and the on/off switch
                try:
                    msg = json.loads(await sock.receive_text())
                except ValueError:
                    continue
                if not isinstance(msg, dict):
                    continue
                if msg.get("action") == "decide" and isinstance(msg.get("id"), str):
                    node.decide(msg["id"], msg.get("accept") is True)
                elif msg.get("action") == "set_online" and isinstance(msg.get("online"), bool):
                    await node.set_online(msg["online"])
        except WebSocketDisconnect:
            pass
        finally:
            pump_task.cancel()
            node.clients.discard(queue)

    return app
