"""DropZone entry point.

    python -m dropzone.main                     # run a node with the web UI
    python -m dropzone.main send HOST:PORT FILE # CLI: send one file
    python -m dropzone.main recv                # CLI: receive, accept in the terminal
"""
import argparse
import asyncio
import json
import os
import random
import shutil
import string
import tempfile
import time
import uuid
from collections import deque
from pathlib import Path

from .discovery import Discovery
from .protocol import ProtocolError, sanitize_filename
from .transfer import TransferServer, send_file


def parse_args(argv=None):
    env = os.environ.get
    p = argparse.ArgumentParser(prog="dropzone", description="LAN file sharing over UDP + TCP")
    # Short friendly default instead of the computer's hostname (which often contains
    # the owner's real name and would be broadcast to the whole LAN).
    p.add_argument("--name", default=env("DZ_NAME") or f"Host {random.choice(string.ascii_uppercase)}",
                   help='node name shown to peers, e.g. "Host A"')
    p.add_argument("--udp-port", type=int, default=int(env("DZ_UDP_PORT", 50000)))
    p.add_argument("--tcp-port", type=int, default=int(env("DZ_TCP_PORT", 50001)))
    p.add_argument("--downloads", default=env("DZ_DOWNLOADS", "downloads"), help="download folder")
    p.add_argument("--web-host", default=env("DZ_WEB_HOST", "127.0.0.1"))
    p.add_argument("--web-port", type=int, default=int(env("DZ_WEB_PORT", 8000)))
    p.add_argument("--max-size", type=int, default=int(env("DZ_MAX_SIZE", 2 * 1024**3)), help="bytes")
    p.add_argument("--beacon-interval", type=float, default=float(env("DZ_BEACON_INTERVAL", 2)),
                   help="seconds between UDP HELLO beacons (offline after 3 missed)")
    p.add_argument("--broadcast", default=env("DZ_BROADCAST", "255.255.255.255"), help="UDP broadcast address")
    sub = p.add_subparsers(dest="cmd")
    sub.add_parser("run", help="run a node with the web UI (default)")
    s = sub.add_parser("send", help="send one file from the command line")
    s.add_argument("target", help="HOST:PORT of the receiver's TCP port")
    s.add_argument("file")
    sub.add_parser("recv", help="receive files, accepting in the terminal")
    return p.parse_args(argv)


class Node:
    """Glue between discovery, transfer and the web UI. Every event goes through emit()."""

    def __init__(self, cfg):
        self.cfg = cfg
        self.id = uuid.uuid4().hex[:12]
        self.clients: set[asyncio.Queue] = set()     # one queue per open browser tab
        self.history = deque(maxlen=300)             # recent inspector lines for new tabs
        self.transfers: dict[str, dict] = {}         # transfer id -> latest progress
        self.pending: dict[str, tuple[dict, asyncio.Future]] = {}  # offers awaiting a click
        self.tasks: set[asyncio.Task] = set()
        self.online = False                          # visible on the network?
        self._toggle_lock = asyncio.Lock()
        self.outbox = Path(tempfile.mkdtemp(prefix="dropzone-outbox-"))
        self.discovery = Discovery(self.id, cfg.name, cfg.tcp_port, cfg.udp_port, cfg.broadcast,
                                   emit=self.emit, on_peers=lambda peers: self.emit("peers", peers),
                                   interval=cfg.beacon_interval)
        self.server = TransferServer(cfg.downloads, self.ask_user, cfg.max_size, emit=self.emit)

    async def set_online(self, on: bool):
        """The per-host on/off switch. Off = BYE, no beacons, no new TCP connections."""
        async with self._toggle_lock:
            if on == self.online:
                return
            if on:
                try:
                    await self.server.start("0.0.0.0", self.cfg.tcp_port)
                    await self.discovery.start()
                except OSError as e:
                    await self.server.close(wait=False)
                    self.emit("state", {"online": False, "error": f"could not go online: {e}"})
                    return
            else:
                for offer_id in list(self.pending):
                    self.decide(offer_id, False)      # decline anything still waiting
                await self.discovery.stop()           # broadcasts BYE
                await self.server.close(wait=False)   # running transfers may finish
            self.online = on
            self.emit("state", {"online": on})

    def received_path(self, name):
        """Path of a finished received file, or None. Used by the preview endpoint."""
        try:
            name = sanitize_filename(name)
        except ProtocolError:
            return None
        folder = self.server.download_dir
        path = folder / name
        if name.endswith(".part") or not path.is_file() or path.resolve().parent != folder:
            return None
        return path

    def emit(self, kind, data):
        event = {"kind": kind, "ts": time.time(), "data": data}
        if kind == "log":
            self.history.append(event)
        elif kind == "progress":
            self.transfers[data["id"]] = data
        for q in self.clients:
            if not q.full():  # a slow tab just misses events instead of using unbounded memory
                q.put_nowait(event)

    async def ask_user(self, offer) -> bool:
        fut = asyncio.get_running_loop().create_future()
        self.pending[offer["id"]] = (offer, fut)
        self.emit("offer", offer)
        try:
            return await fut
        finally:
            self.pending.pop(offer["id"], None)
            self.emit("offer_closed", {"id": offer["id"]})

    def decide(self, offer_id, accept):
        entry = self.pending.get(offer_id)
        if entry and not entry[1].done():
            entry[1].set_result(accept is True)

    def send_to_peer(self, peer, tmp_path: Path, name: str) -> str:
        tid = uuid.uuid4().hex  # returned to the browser so it can preview its own copy later

        async def job():
            try:
                await send_file(peer["ip"], peer["tcp_port"], tmp_path, name=name, emit=self.emit, tid=tid)
            except Exception:
                pass  # already reported to the UI as a failed progress event
            finally:
                tmp_path.unlink(missing_ok=True)
        task = asyncio.create_task(job())
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return tid

    def snapshot(self):
        return {"self": {"id": self.id, "name": self.cfg.name, "tcp_port": self.cfg.tcp_port},
                "online": self.online,
                "peers": self.discovery.snapshot(),
                "offers": [o for o, _ in self.pending.values()],
                "transfers": list(self.transfers.values()),
                "log": list(self.history)}


async def run_node(cfg):
    import uvicorn
    from .web import create_app

    node = Node(cfg)
    await node.set_online(True)
    if not node.online:
        raise SystemExit(f"Could not open UDP {cfg.udp_port} / TCP {cfg.tcp_port}")
    server = uvicorn.Server(uvicorn.Config(create_app(node), host=cfg.web_host,
                                           port=cfg.web_port, log_level="warning"))
    print(f"DropZone '{cfg.name}': UDP {cfg.udp_port}, TCP {cfg.tcp_port}, "
          f"UI http://{cfg.web_host}:{cfg.web_port}  (downloads -> {Path(cfg.downloads).resolve()})")
    try:
        # uvicorn runs on the *same* asyncio loop as the UDP and TCP servers.
        await server.serve()
    finally:
        await node.set_online(False)  # sends BYE, declines waiting offers
        shutil.rmtree(node.outbox, ignore_errors=True)


def print_event(kind, data):
    if kind == "log":
        print(f"{time.strftime('%H:%M:%S')} {data['proto']} {data['dir']:>3} {data['peer']}  {json.dumps(data['msg'])}")
    elif kind == "progress" and data["status"] in ("done", "failed", "rejected"):
        print(f"  {data['name']}: {data['status']} {data['done']}/{data['size']} B, "
              f"{data['mbps']} MB/s {data['error'] or ''}")


async def cli_recv(cfg):
    async def ask(offer):
        answer = await asyncio.to_thread(
            input, f"Accept '{offer['name']}' ({offer['size']} bytes) from {offer['peer']}? [y/N] ")
        return answer.strip().lower() == "y"
    server = TransferServer(cfg.downloads, ask, cfg.max_size, emit=print_event)
    await server.start("0.0.0.0", cfg.tcp_port)
    print(f"Receiving on TCP {cfg.tcp_port} into {server.download_dir}  (Ctrl-C to stop)")
    await asyncio.Event().wait()


async def cli_send(cfg):
    host, _, port = cfg.target.rpartition(":")
    ok = await send_file(host, int(port), cfg.file, emit=print_event)
    raise SystemExit(0 if ok else 1)


def main(argv=None):
    cfg = parse_args(argv)
    cmd = {"send": cli_send, "recv": cli_recv}.get(cfg.cmd, run_node)
    try:
        asyncio.run(cmd(cfg))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
