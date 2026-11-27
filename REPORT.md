# DropZone: LAN File Sharing over UDP and TCP

**GitHub:** https://github.com/Kuro1105/dropzone

## 1. Introduction

Moving a file between two computers in the same room is still clumsy. Cloud drives send the file to a
server far away and back, USB sticks get lost, and AirDrop only works between Apple devices. I built
**DropZone**, a small AirDrop-style file sharer written in Python. Each computer runs one node. Nodes find
each other automatically with **UDP broadcast**, and files move directly between them over **TCP**. The
user controls everything from a web page on `127.0.0.1`: pick a nearby host, drop a file, and the
receiver clicks Accept. A built-in network inspector shows every UDP beacon and TCP control message as it
happens, so the networking is visible, not hidden.

## 2. Why UDP and why TCP

The two jobs have opposite needs, so I used a different transport for each.

**Discovery** asks "who is out there?" and must reach *everyone*. UDP supports broadcast: one `sendto()`
to `255.255.255.255:50000` reaches every host on the subnet, with no connection setup. Losing a beacon
doesn't matter, because another one follows 2 seconds later.

**Transfer** must move one file to *one* host, completely and in the right order. TCP gives a reliable,
ordered byte stream with retransmission and flow control. Rebuilding that on top of UDP for a 2 GB file
would mean re-implementing TCP.

| | UDP (discovery, port 50000) | TCP (transfer, port 50001) |
|---|---|---|
| Connection | None | 3-way handshake, one per transfer |
| Recipients | One-to-many (broadcast) | One-to-one |
| Delivery | No guarantee; a lost beacon is fine | Reliable, ordered, retransmitted |
| Flow control | None | Yes (receive window) |
| Used for | HELLO / BYE beacons | OFFER / ACCEPT / file bytes / DONE |

## 3. Architecture

The code is about 850 lines of Python plus a plain HTML/CSS/JS page. Everything runs on a single
`asyncio` event loop: UDP, the TCP server, outgoing transfers and the web server (uvicorn).

```
  Browser (127.0.0.1)
      |  HTTP upload + WebSocket events
  +---v-----------------------------------------------+
  | web.py      FastAPI page, /ws, /api/send, preview |
  | main.py     Node: config, wiring, on/off, CLI     |
  |   |                         |                     |
  | discovery.py             transfer.py              |
  | UDP 50000                TCP 50001                |
  | HELLO/BYE, peer table    server + send_file       |
  |        \                  /                       |
  |         protocol.py: framing, validation,         |
  |         filename sanitising, SHA-256              |
  +---------------------------------------------------+
        | UDP broadcast            | TCP connection
        v                          v
     every node on the LAN      one chosen peer
```

- **protocol.py** holds the message framing and checks every incoming field.
- **discovery.py** sends beacons and keeps the peer table.
- **transfer.py** receives files (server) and sends them (`send_file`).
- **web.py** serves the UI and streams browser uploads to a temp file, so a file is never loaded into memory.

## 4. Protocol design

**Beacons.** Every 2 seconds each node broadcasts:

```json
{"type":"HELLO","id":"5e22d0488f27","name":"Host A","tcp_port":50001,"ver":1}
```

The peer's IP comes from the packet's source address, so it isn't trusted from the payload. A peer that
misses 3 beacons (6 s) is marked offline. On shutdown a node broadcasts `{"type":"BYE","id":...}`. When a
node hears a peer it doesn't know yet, it answers with one HELLO right away, so a newcomer doesn't wait
for the next interval.

**Framing.** TCP is a byte stream, not a sequence of messages. One `send()` of 100 bytes can arrive as
three reads of 30, 30 and 40 bytes, or merged with the next message. So each control message is
prefixed with its length:

```
+----------------------+----------------------+
| length (4 bytes, BE) | JSON body (<= 64 KB) |
+----------------------+----------------------+
```

The receiver reads exactly 4 bytes, checks the length against the 64 KB limit *before* reading the
body, then reads exactly that many bytes (`readexactly`). A test feeds the stream in 3-byte pieces to
prove this works.

**Flow.**

1. The sender connects and sends `OFFER {name, size, sha256}`.
2. The receiver's UI shows the offer. The user either rejects it (`REJECT`) or accepts it, and the
   receiver replies `ACCEPT {offset}`.
3. The sender streams the raw bytes from `offset` in 64 KB chunks.
4. The receiver checks the SHA-256 and replies `DONE {ok}`.

**Resume.** Incoming data is written to `name.<first 12 hex of hash>.part`. If the connection drops, the
`.part` file is kept. When the same file is offered again, the receiver finds the matching `.part`,
hashes what it already has, and answers `ACCEPT {offset: <bytes already there>}`. Only the missing tail
is sent. The file is renamed to its real name only after the full hash matches; on a mismatch the
`.part` is deleted.

## 5. Security measures

- The web UI binds to `127.0.0.1` by default and rejects cross-origin requests and WebSockets (Origin check).
- Every transfer must be accepted by the receiver. Nothing is written before that.
- File names are reduced to a bare name (`../../x` → `x`). Empty, `.`, `..` and control characters are
  rejected. Files are written only inside the download folder and never overwrite an existing file
  (`a.txt` → `a (1).txt`).
- Control messages over 64 KB and files over the configurable maximum (default 2 GB) are rejected.
- Every JSON field is type- and range-checked, including the sender checking the receiver's `offset`.
  Malformed UDP packets are silently ignored.
- TCP reads time out after 30 s, and an unanswered offer is declined after 120 s.
- Files are streamed in chunks. No pickle, `eval` or shell calls touch network data.
- The preview endpoint serves only finished files, with `Content-Security-Policy: sandbox` so a received
  HTML/SVG file can't run scripts against the UI.

There is no encryption or authentication, so anyone on the LAN can read the traffic or send an offer
(which can still be rejected).

## 6. Testing and results

**Automated tests:** `pytest` reports **38 passed** (22 test functions, some parametrized). They cover:
- framing with a split stream, plus oversized, malformed and timed-out messages;
- filename sanitising and beacon validation;
- real localhost transfers with a hash check, resume from a partial file, reject and too-large (nothing
  written), path traversal, no-overwrite, a corrupt partial, a stalled sender, and a receiver sending a
  bogus offset;
- the on/off switch.

I also ran the README's commands from a fresh clone with a new virtual environment, and they worked as
written.

**Measurements.** I ran three Docker nodes on one bridge network and measured them with a script that
drives the same web endpoints the UI uses. I ran it 3 times:

| Measurement | Result (3 runs) |
|---|---|
| New node appears on another host (from `docker compose start`) | 0.37–0.42 s |
| Stopped node marked offline (sends BYE) | 0.40–0.48 s |
| Frozen node marked offline (`docker compose pause`, no BYE) | 5.0–5.6 s |
| 100 MB file, Host A → Host B, accept to DONE | 0.30–0.40 s = 259–353 MB/s |
| SHA-256 of received file | matched in 3/3 runs |

The silent-node result fits the design: the peer had last beaconed 0–2 s before it was frozen, the
timeout is 6 s, and the check runs once per second. The transfer speed is high because all three
containers share one virtual machine, so no physical network is involved. On real Wi-Fi the speed would
be limited by the link.

[Screenshot: UDP beacons]

[Screenshot: Wireshark TCP handshake]

[Screenshot: Web UI transfer]

## 7. Use of agentic AI

I built DropZone with an AI coding agent (Claude Code). I wrote the specification: the ports, message
format, security rules, file structure and the rule to keep the code small. The agent generated the code,
tests, Docker setup and README, and kept a step-by-step log (`AI_LOG.md`). After each step it checked
the result by running the code, not just by reading it. That included CLI transfers with hash
comparisons, three-node Docker runs and screenshots from a headless browser. I then asked for changes:
a redesign, "Host A/B/C" names (the default had broadcast the computer's real hostname to the whole
LAN), an Online/Offline switch and file previews.

Real bugs came up and were fixed:
- The UI got no event when a TCP connection failed.
- A restarted container showed up twice.
- A CSS class name clashed between two parts of the page.
- A browser kept an old cached stylesheet after an update.

The agent also made mistakes. When I raised the beacon interval to 15 s, its verification scripts hung
because of their own timeout handling. The app was not at fault, but I reverted to 2 s. During testing,
files went missing from two download folders. The cause was never confirmed, and the agent first wrongly
reported them as untouched; only one could be restored. This taught me to check the agent's claims
myself, especially about my own files.

## 8. Challenges and what I learned

- **TCP has no message boundaries.** I had assumed one send equals one receive. Length-prefix framing,
  and checking the length before allocating, fixed that and also blocks memory attacks.
- **Broadcast inside Docker.** I expected to need the subnet broadcast address, but `255.255.255.255`
  works on a Docker bridge. Several nodes on one machine also need `SO_REUSEPORT` to share UDP 50000.
- **Peer identity.** A restarted node gets a new id, so the peer table kept a stale "ghost" entry.
  Treating the same IP and TCP port as the same machine fixed it.
- **Timing trade-offs.** A longer beacon interval means less traffic but slower offline detection. The
  immediate HELLO reply keeps discovery fast whatever the interval is.

## 9. Conclusion and future work

DropZone shows each transport doing the job it suits: UDP broadcast to find peers cheaply, and TCP to
move files reliably, with a verified hash and resume. The tests and measurements show it works, and the
inspector plus Wireshark make the protocol easy to see.

Future work:
- Encryption and peer authentication (for example TLS with pairing codes).
- A mobile client.
- Sending several files or a whole folder in one offer.
- Discovery across subnets (for example with mDNS).
