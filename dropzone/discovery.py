"""Peer discovery over UDP broadcast.

Why UDP here: discovery is "shout to everyone on the LAN, nobody in particular".
UDP supports broadcast (TCP can only talk to one known host), needs no connection
setup, and losing a beacon is harmless because another one follows shortly after.
"""
import asyncio
import json
import socket
import time

from .protocol import VERSION, parse_beacon

BEACON_INTERVAL = 2.0    # seconds between HELLO broadcasts
MISSED_BEACONS = 3       # silent for 3 intervals (6 s) -> offline


class Discovery(asyncio.DatagramProtocol):
    def __init__(self, node_id, name, tcp_port, udp_port=50000,
                 broadcast="255.255.255.255", emit=None, on_peers=None, interval=BEACON_INTERVAL):
        self.node_id, self.name, self.tcp_port = node_id, name, tcp_port
        self.interval, self.timeout = interval, interval * MISSED_BEACONS
        self.hello = {"type": "HELLO", "id": node_id, "name": name, "tcp_port": tcp_port, "ver": VERSION}
        self.udp_port, self.broadcast = udp_port, broadcast
        self.emit = emit or (lambda *a: None)
        self.on_peers = on_peers or (lambda peers: None)
        self.peers: dict[str, dict] = {}   # id -> {id, name, ip, tcp_port, online, last_seen}
        self.transport = None
        self._tasks = []

    async def start(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # REUSEADDR/REUSEPORT let several DropZone nodes on the same machine all
        # bind UDP 50000; every one of them receives each broadcast datagram.
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        # The OS refuses to send to a broadcast address unless we opt in.
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind(("", self.udp_port))  # "" = all interfaces, so we hear broadcasts
        sock.setblocking(False)
        loop = asyncio.get_running_loop()
        self.transport, _ = await loop.create_datagram_endpoint(lambda: self, sock=sock)
        self._tasks = [asyncio.create_task(self._beacon_loop()),
                       asyncio.create_task(self._reap_loop())]

    async def stop(self):
        for t in self._tasks:
            t.cancel()
        self._tasks = []
        if self.transport:
            self._send({"type": "BYE", "id": self.node_id})  # tell peers right away
            await asyncio.sleep(0.05)  # give the datagram a moment to leave
            self.transport.close()
            self.transport = None
        # We no longer listen, so the table would only go stale.
        self.peers.clear()
        self.on_peers([])

    def online_peers(self):
        return [p for p in self.peers.values() if p["online"]]

    def snapshot(self):
        return [{k: v for k, v in p.items() if k != "last_seen"} for p in self.peers.values()]

    # ------------------------------------------------------------ sending
    def _send(self, msg: dict):
        # One sendto() to the broadcast address reaches every host on the subnet.
        self.transport.sendto(json.dumps(msg).encode(), (self.broadcast, self.udp_port))
        self.emit("log", {"proto": "UDP", "dir": "out",
                          "peer": f"{self.broadcast}:{self.udp_port}", "msg": msg})

    async def _beacon_loop(self):
        while True:
            self._send(self.hello)
            await asyncio.sleep(self.interval)

    async def _reap_loop(self):
        while True:
            await asyncio.sleep(1)
            now = time.monotonic()
            changed = False
            for p in self.peers.values():
                if p["online"] and now - p["last_seen"] > self.timeout:
                    p["online"] = False
                    changed = True
            if changed:
                self.on_peers(self.snapshot())

    # ------------------------------------------------------------ receiving
    def datagram_received(self, data, addr):
        msg = parse_beacon(data)
        if msg is None or msg["id"] == self.node_id:  # junk, or our own echo
            return
        self.emit("log", {"proto": "UDP", "dir": "in", "peer": f"{addr[0]}:{addr[1]}", "msg": msg})
        old = self.peers.get(msg["id"])
        if msg["type"] == "BYE":
            if old and old["online"]:
                old["online"] = False
                self.on_peers(self.snapshot())
            return
        # The peer's IP comes from the UDP packet itself, not from the payload.
        peer = {"id": msg["id"], "name": msg["name"], "ip": addr[0],
                "tcp_port": msg["tcp_port"], "online": True, "last_seen": time.monotonic()}
        # A restarted node gets a new id; drop its stale entry at the same address.
        stale = [pid for pid, p in self.peers.items() if pid != peer["id"]
                 and (p["ip"], p["tcp_port"]) == (peer["ip"], peer["tcp_port"])]
        for pid in stale:
            del self.peers[pid]
        self.peers[msg["id"]] = peer
        if old is None:
            # Someone new: answer right away so they see us now, not one interval later.
            # Only for unknown peers, so this can't turn into a broadcast storm.
            self._send(self.hello)
        if stale or old is None or any(old[k] != peer[k] for k in ("name", "ip", "tcp_port", "online")):
            self.on_peers(self.snapshot())

    def error_received(self, exc):
        self.emit("log", {"proto": "UDP", "dir": "err", "peer": "-", "msg": {"error": str(exc)}})
