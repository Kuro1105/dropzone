# AI log

Running log of what the AI assistant (Claude) built and checked at each step.

1. **Setup.** Python 3.14 venv with fastapi, uvicorn[standard] (for WebSocket support), pytest, pytest-asyncio.
2. **protocol.py.** Length-prefixed JSON framing (`!I` header + body, 64 KB cap checked
   *before* reading the body), `parse_beacon`/`parse_offer`/`parse_accept`/`parse_done`
   validators, `sanitize_filename`, `unique_path`, chunked `sha256_file`.
   Checked by hand: traversal names (`../../etc/passwd`, `..\..\win.ini`), NUL bytes, bool-as-int ports.
3. **discovery.py.** UDP socket with SO_REUSEADDR + SO_REUSEPORT + SO_BROADCAST, HELLO every 2 s,
   reaper marks peers offline after 6 s, BYE on stop. Checked: two processes on one Mac found
   each other and BYE marked the peer offline.
4. **transfer.py + CLI.** TCP server/sender with OFFER → ACCEPT/REJECT → bytes → DONE, per-read
   timeouts, `.part` files keyed by name + hash prefix, resume from offset, rename after the hash
   matches. Checked with `recv`/`send`: 20 MB accept (hashes match), reject, and resume from a
   7 MB partial (only 13 MB sent, saved as `big (1).bin` without overwriting).
   - *Fix:* if the TCP connect failed, the UI got no event, because the progress record was created
     after connecting. It is now created before connecting.
5. **web.py + static UI.** FastAPI page, `/ws` event stream (snapshot and then live events), `/api/send`
   streams the browser upload to a temp file, then sends it over TCP. Added an Origin check so other
   websites can't drive the local UI. Checked with a scripted end-to-end run: a `../../evil.bin` name was saved as
   `evil.bin`, and a cross-origin WS or POST is refused (403).
6. **Inspector.** All UDP/TCP events go through `Node.emit`; the UI shows time, protocol,
   direction, peer, type and body, with UDP/TCP/own-beacon filters and pause.
7. **Docker.** 3 nodes on a bridge network, UIs on 127.0.0.1:8001-8003, non-root user.
   Checked that `255.255.255.255` broadcast works on the bridge (no subnet address needed).
   Checked a node1→node2 transfer (hash matches), a paused node going offline after about 6 s,
   and BYE on `docker compose stop`.
   - *Fix:* a restarted container got a new id and showed up twice (old entry offline).
     A HELLO from the same ip:tcp_port now replaces the stale entry.
   - Checked UI screenshots in headless Chrome (peer list, pending offer, progress, inspector).
8. **Tests.** 35 tests: framing (split stream), oversized/malformed/timeout, sanitisation, beacon
   validation, full transfer, resume, reject, too large, traversal, no-overwrite, corrupt partial,
   stalled sender, bogus ACCEPT offset. All pass.
9. **README.** Includes a Wireshark guide; checked the Docker `tcpdump` capture recipe.
10. **Redesign.** Warm orange/cream theme with colourful peer cards, pill buttons and a dark-indigo
    inspector. Checked in headless Chrome (light, dark, 375 px wide).
    - *Fix:* inspector address cells shared the `.peer` class and were drawn as peer cards; renamed to `.addr`.
11. **Names.** Nodes are called "Host A/B/C" in Docker; a local node defaults to "Host <letter>"
    instead of the full hostname (which was broadcast to the whole LAN).
12. **Beacon interval 2 s → 15 s** (configurable, `--beacon-interval`). Offline timeout is now
    3 missed beacons (45 s). Added an immediate HELLO reply when a node sees an unknown peer, so
    newcomers still appear within a second. README updated.
13. **Beacon interval back to 2 s** by request (offline after 6 s). Kept `--beacon-interval` and the
    immediate HELLO reply to new peers. (The earlier verification "hangs" were bugs in my test
    scripts' timeout handling, not in the app.)
14. **Visible downloads for Docker.** Named volumes lived inside the Docker VM, so received files
    never showed up in the project folder. Switched to bind mounts `./received/host-a|b|c`, copied the
    already-received files over, and checked that a Host C → Host B transfer appears in `received/host-b/`
    with a matching hash.
15. **Host on/off switch + file preview.** `Node.set_online()` sends BYE, stops discovery, closes the
    TCP listener and declines pending offers (shutdown reuses it, so a waiting offer no longer delays
    exit). New `/api/files/<name>` serves finished received files with a CSP sandbox; the sender
    previews its own `File` via a blob URL. 3 new tests (38 total). Checked in Docker and in headless
    Chrome driven through the DevTools protocol (real clicks on the switch and Preview, real file picker).
    - *Fix:* async pytest fixture needed `@pytest_asyncio.fixture` in strict mode.
16. **UI refinements.** The status pill is now the Online/Offline switch (click → menu with both
    options); the separate switch is gone. Preview dialog restyled to match the page (orange header
    band with sun arcs, file-type chip, pill buttons). Added a "technical details" toggle (`.tech`
    elements, remembered in localStorage) and friendly sender names ("from Host A"). Checked with
    real clicks in headless Chrome: menu, keyboard Escape, offline/online, simple mode surviving a
    reload, image/text/unknown previews, dark mode and 375 px width.
