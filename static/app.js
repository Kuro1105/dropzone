// DropZone UI. Everything live arrives over one WebSocket from the local node.
// All peer-supplied text is inserted with textContent, never innerHTML.
const $ = (s) => document.querySelector(s);
let ws, me = null, peers = [], selected = null;
let hostOn = true, wsUp = false, switching = false;
const transfers = new Map();   // id -> <li>
const localFiles = new Map();  // transfer id -> File we sent (for previewing our own copy)
const MAX_LOG = 500;

function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  return e;
}
const tech = (text) => el("span", "tech", text);  // hidden when "technical details" is off
const fmtBytes = (n) => n >= 1e9 ? (n / 1e9).toFixed(2) + " GB" : n >= 1e6 ? (n / 1e6).toFixed(1) + " MB"
  : n >= 1e3 ? (n / 1e3).toFixed(1) + " KB" : n + " B";
const fmtTime = (ts) => new Date(ts * 1000).toTimeString().slice(0, 8) + "." + String(Math.floor(ts * 1000) % 1000).padStart(3, "0");

function setEmpty(list, text) {
  if (!list.children.length) list.append(el("li", "empty", text));
  else if (list.children.length > 1) list.querySelector(".empty")?.remove();
}

// "172.19.0.4:38084" -> "Host A" when we can tell which peer it is.
function nameFor(addr) {
  const i = addr.lastIndexOf(":"), ip = addr.slice(0, i), port = Number(addr.slice(i + 1));
  const exact = peers.find((p) => p.ip === ip && p.tcp_port === port);
  if (exact) return exact.name;
  const sameIp = peers.filter((p) => p.ip === ip);
  return sameIp.length === 1 ? sameIp[0].name : null;  // several hosts on one machine: can't tell
}
// [friendly name, technical address] for display
const who = (addr) => [nameFor(addr) || "a nearby host", tech(` · ${addr}`)];

// ------------------------------------------------------------------ socket
function connect() {
  ws = new WebSocket(`${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws`);
  ws.onopen = () => { wsUp = true; renderStatus(); };
  ws.onclose = () => { wsUp = false; renderStatus(); setTimeout(connect, 1500); };
  ws.onmessage = (e) => handle(JSON.parse(e.data));
}

function handle(ev) {
  const d = ev.data;
  switch (ev.kind) {
    case "snapshot":
      me = d.self;
      $("#me").replaceChildren(el("b", "", me.name), tech(` · id ${me.id} · TCP ${me.tcp_port}`));
      $("#offers").replaceChildren(); $("#transfers").replaceChildren(); $("#log").replaceChildren();
      transfers.clear();
      peers = d.peers;
      setHostOn(d.online);
      d.offers.forEach(addOffer); setEmpty($("#offers"), "Nothing waiting.");
      d.transfers.forEach(updateTransfer); setEmpty($("#transfers"), "No transfers yet.");
      d.log.forEach(addLog);
      break;
    case "peers": peers = d; renderPeers(); break;
    case "state":
      setHostOn(d.online);
      if (d.error) alert(d.error);
      break;
    case "offer": addOffer(d); break;
    case "offer_closed":
      document.getElementById("offer-" + d.id)?.remove();
      setEmpty($("#offers"), "Nothing waiting.");
      break;
    case "progress": updateTransfer(d); break;
    case "log": addLog(ev); break;
  }
}

// ------------------------------------------------------------------ status pill = on/off switch
function setHostOn(on) {
  hostOn = on;
  switching = false;
  document.body.classList.toggle("host-off", !on);
  renderStatus();
  renderPeers();
}

function renderStatus() {
  const btn = $("#status");
  const busy = !wsUp || switching;
  btn.className = "status " + (busy ? "wait" : hostOn ? "on" : "off");
  btn.disabled = busy;
  $("#status-text").textContent = !wsUp ? "Reconnecting…" : switching ? "Switching…" : hostOn ? "Online" : "Offline";
  for (const opt of document.querySelectorAll("#status-menu .opt")) {
    opt.setAttribute("aria-checked", String((opt.dataset.online === "true") === hostOn));
  }
  if (busy) toggleMenu(false);
}

function toggleMenu(open) {
  const menu = $("#status-menu");
  open = open ?? menu.hidden;
  menu.hidden = !open;
  $("#status").setAttribute("aria-expanded", String(open));
  if (open) menu.querySelector('[aria-checked="true"]')?.focus();
}

$("#status").onclick = (e) => { e.stopPropagation(); toggleMenu(); };
document.addEventListener("click", (e) => { if (!e.target.closest(".status-wrap")) toggleMenu(false); });
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape" && !$("#status-menu").hidden) { toggleMenu(false); $("#status").focus(); }
});
for (const opt of document.querySelectorAll("#status-menu .opt")) {
  opt.onclick = () => {
    const on = opt.dataset.online === "true";
    toggleMenu(false);
    if (on === hostOn) return;
    switching = true;  // cleared when the node confirms with a "state" event
    renderStatus();
    ws.send(JSON.stringify({ action: "set_online", online: on }));
  };
}

// ------------------------------------------------------------------ technical details toggle
function setTech(show) {
  document.body.classList.toggle("simple", !show);
  $("#tech-toggle").setAttribute("aria-pressed", String(show));
  $("#tech-label").textContent = show ? "Hide technical details" : "Show technical details";
  try { localStorage.setItem("dz-tech", show ? "1" : "0"); } catch { /* private mode etc. */ }
}
let showTech = true;
try { showTech = localStorage.getItem("dz-tech") !== "0"; } catch { /* default: shown */ }
setTech(showTech);
$("#tech-toggle").onclick = () => setTech(document.body.classList.contains("simple"));

// ------------------------------------------------------------------ peers
function renderPeers() {
  const list = $("#peers");
  list.replaceChildren();
  if (!hostOn) {
    selected = null;
    list.append(el("li", "empty", "You're offline, so you can't see or reach anyone. Click “Offline” at the top to go online."));
    $("#drop").classList.remove("ready");
    $("#drop-text").textContent = "Go online to send files";
    return;
  }
  if (selected && !peers.some((p) => p.id === selected && p.online)) selected = null;
  for (const [i, p] of peers.entries()) {
    const li = el("li", `peer c${i % 6}` + (p.online ? "" : " offline") + (p.id === selected ? " selected" : ""));
    const text = el("div", "peer-text");
    text.append(el("b", "", p.name), el("span", "sub tech", `${p.ip}:${p.tcp_port}`));
    li.append(el("span", "avatar", p.name.trim().split(/\s+/).pop().slice(0, 1).toUpperCase()), text,
      el("span", "state", p.id === selected ? "Selected" : p.online ? "Online" : "Offline"));
    if (p.online) {
      li.tabIndex = 0;
      li.onclick = () => { selected = p.id; renderPeers(); };
      li.onkeydown = (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); li.onclick(); } };
    }
    list.append(li);
  }
  setEmpty(list, "Looking for peers…");
  const peer = peers.find((p) => p.id === selected);
  $("#drop").classList.toggle("ready", !!peer);
  $("#drop-text").textContent = peer ? `Drop a file to send to ${peer.name}, or click to choose`
    : "Pick a peer, then drop a file here or click to choose";
}

// ------------------------------------------------------------------ sending
async function sendFile(file) {
  const peer = peers.find((p) => p.id === selected);
  if (!peer) return alert("Pick an online peer first.");
  $("#drop-text").textContent = `Preparing ${file.name}…`;
  // The browser streams the file to its own node; the node sends it over TCP.
  const res = await fetch(`/api/send?peer=${encodeURIComponent(peer.id)}&name=${encodeURIComponent(file.name)}`,
    { method: "POST", body: file }).catch((e) => ({ ok: false, statusText: e.message }));
  if (!res.ok) alert(`Could not send: ${res.status || ""} ${res.statusText}`);
  else {
    const { id } = await res.json();
    localFiles.set(id, file);
    const li = transfers.get(id);
    if (li) updateTransfer(li.t);  // show the Preview button if it already finished
  }
  renderPeers();
}

const drop = $("#drop");
$("#file").onchange = (e) => { if (e.target.files[0]) sendFile(e.target.files[0]); e.target.value = ""; };
drop.addEventListener("click", (e) => {
  if (!hostOn) { e.preventDefault(); alert("You're offline. Click the status pill at the top to go online."); }
  else if (!selected) { e.preventDefault(); alert("Pick an online peer first."); }
});
drop.ondragover = (e) => { e.preventDefault(); drop.classList.add("over"); };
drop.ondragleave = () => drop.classList.remove("over");
drop.ondrop = (e) => {
  e.preventDefault(); drop.classList.remove("over");
  if (e.dataTransfer.files[0]) sendFile(e.dataTransfer.files[0]);
};

// ------------------------------------------------------------------ offers
function addOffer(o) {
  const li = el("li", "offer");
  li.id = "offer-" + o.id;
  const info = el("div");
  const meta = el("div", "meta", `${fmtBytes(o.size)} from `);
  meta.append(...who(o.peer));
  info.append(el("b", "", o.name), meta, el("div", "meta mono tech", "sha256 " + o.sha256.slice(0, 16) + "…"));
  const yes = el("button", "btn primary", "Accept"), no = el("button", "btn outline", "Reject");
  const decide = (accept) => { ws.send(JSON.stringify({ action: "decide", id: o.id, accept })); li.remove(); };
  yes.onclick = () => decide(true);
  no.onclick = () => decide(false);
  const btns = el("div", "btns"); btns.append(yes, no);
  li.append(info, btns);
  $("#offers").prepend(li);
  setEmpty($("#offers"), "Nothing waiting.");
}

// ------------------------------------------------------------------ progress
function updateTransfer(t) {
  let li = transfers.get(t.id);
  if (!li) {
    li = el("li", "transfer");
    li.innerHTML = '<div class="t-head"><span class="t-icon"></span><div class="t-text"><b></b>' +
      '<span class="who"></span></div><span class="t-pct"></span></div>' +
      '<div class="bar"><div></div></div>' +
      '<div class="t-foot"><div class="stat"></div><button class="btn small" hidden>Preview</button></div>';
    li.querySelector("button").onclick = () => openPreview(li.t);
    transfers.set(t.id, li);
    $("#transfers").prepend(li);
    setEmpty($("#transfers"), "No transfers yet.");
  }
  li.t = t;
  const pct = t.size ? Math.floor((100 * t.done) / t.size) : 100;
  // Sender previews the File still held by this tab; receiver previews the saved copy.
  li.querySelector("button").hidden = !(t.status === "done" && (localFiles.has(t.id) || (t.dir === "in" && t.saved)));
  li.querySelector("b").textContent = t.name;
  li.querySelector(".who").replaceChildren(t.dir === "out" ? "to " : "from ", ...who(t.peer));
  li.querySelector(".t-icon").textContent = t.dir === "out" ? "↑" : "↓";
  li.querySelector(".t-pct").textContent = pct + "%";
  li.querySelector(".bar div").style.width = pct + "%";
  li.className = "transfer " + t.status;
  const label = { waiting: "waiting for them to accept", active: "", done: "done ✓",
    rejected: "declined", failed: "failed" }[t.status] ?? t.status;
  li.querySelector(".stat").replaceChildren(
    `${fmtBytes(t.done)} / ${fmtBytes(t.size)} · ${t.mbps} MB/s${label ? " · " + label : ""}`,
    t.status === "done" ? tech(" SHA-256 verified") : "",
    t.error ? ` (${t.error})` : "");
}

// ------------------------------------------------------------------ inspector
function addLog(ev) {
  if ($("#f-pause").checked) return;
  const d = ev.data, m = d.msg;
  const own = d.proto === "UDP" && d.dir === "out";
  const row = el("div", `entry ${d.proto.toLowerCase()} ${d.dir}${own ? " own" : ""}`);
  const arrow = { out: "→ sent", in: "← recv", err: "! error" }[d.dir];
  const { type, ...rest } = m;
  row.append(el("span", "ts", fmtTime(ev.ts)), el("span", "proto", d.proto), el("span", "dir", arrow),
    el("span", "addr", d.peer), el("span", "type", type || ""), el("span", "body", Object.keys(rest).length ? JSON.stringify(rest) : ""));
  const log = $("#log");
  const atBottom = log.scrollHeight - log.scrollTop - log.clientHeight < 40;
  log.append(row);
  while (log.children.length > MAX_LOG) log.firstChild.remove();
  if (atBottom) log.scrollTop = log.scrollHeight;
}

for (const [id, cls] of [["#f-udp", "hide-udp"], ["#f-tcp", "hide-tcp"], ["#f-own", "hide-own"]]) {
  $(id).onchange = (e) => $("#log").classList.toggle(cls, !e.target.checked);
}
$("#clear").onclick = () => $("#log").replaceChildren();

// ------------------------------------------------------------------ preview
const PREVIEW_TEXT_LIMIT = 200_000;  // bytes of a text file to show
const KIND_LABEL = { image: "Image", video: "Video", audio: "Audio", pdf: "PDF", text: "Text" };
function kindOf(name, mime = "") {
  const ext = name.split(".").pop().toLowerCase();
  if (mime.startsWith("image/") || /^(png|jpe?g|gif|webp|bmp|svg|avif|ico)$/.test(ext)) return "image";
  if (mime.startsWith("video/") || /^(mp4|webm|mov|m4v|ogv)$/.test(ext)) return "video";
  if (mime.startsWith("audio/") || /^(mp3|wav|ogg|m4a|aac|flac)$/.test(ext)) return "audio";
  if (mime === "application/pdf" || ext === "pdf") return "pdf";
  if (mime.startsWith("text/") || /^(txt|md|csv|tsv|json|log|py|js|ts|html?|css|xml|ya?ml|ini|toml|c|h|cpp|java|go|rs|sh|sql)$/.test(ext)) return "text";
  return null;
}

let previewUrl = null;
async function openPreview(t) {
  const local = localFiles.get(t.id);
  const name = local ? t.name : t.saved;
  previewUrl = local ? URL.createObjectURL(local) : `/api/files/${encodeURIComponent(name)}`;
  const kind = kindOf(name, local?.type);
  const ext = name.includes(".") ? name.split(".").pop().toUpperCase().slice(0, 5) : "FILE";

  $("#pv-chip").textContent = `${t.dir === "out" ? "↑ Sent" : "↓ Received"} · ${KIND_LABEL[kind] || ext}`;
  $("#pv-title").textContent = name;
  $("#pv-meta").replaceChildren(`${fmtBytes(t.size)} · ${t.dir === "out" ? "to" : "from"} `, ...who(t.peer));
  $("#pv-note").textContent = t.dir === "out" ? "Your original file" : "Saved in this host's download folder";
  $("#pv-download").href = previewUrl;
  $("#pv-download").download = name;

  const body = $("#pv-body");
  const frame = el("div", "pv-frame " + (kind || "none"));
  body.replaceChildren(frame);
  if (kind === "image") frame.append(Object.assign(el("img"), { src: previewUrl, alt: name }));
  else if (kind === "video") frame.append(Object.assign(el("video"), { src: previewUrl, controls: true }));
  else if (kind === "audio") {
    frame.append(el("span", "pv-badge", "♪"), Object.assign(el("audio"), { src: previewUrl, controls: true }));
  } else if (kind === "pdf") frame.append(Object.assign(el("iframe"), { src: previewUrl, title: name }));
  else if (kind === "text") {
    const pre = el("pre", "", "Loading…");
    frame.append(pre);
    const text = local ? await local.slice(0, PREVIEW_TEXT_LIMIT).text()
      : await fetch(previewUrl, { headers: { Range: `bytes=0-${PREVIEW_TEXT_LIMIT - 1}` } }).then((r) => r.text());
    pre.textContent = text + (t.size > PREVIEW_TEXT_LIMIT ? "\n\n… (only the first 200 KB shown)" : "");
  } else {
    frame.append(el("span", "pv-badge", ext), el("b", "", "No preview for this kind of file"),
      el("span", "", "Download it to open it on your computer."));
  }
  $("#preview").showModal();
}
const closePreview = () => $("#preview").close();
$("#pv-close").onclick = closePreview;
$("#pv-done").onclick = closePreview;
$("#preview").onclick = (e) => { if (e.target === e.currentTarget) closePreview(); };  // click on backdrop
$("#preview").onclose = () => {
  $("#pv-body").replaceChildren();  // stops video/audio
  if (previewUrl?.startsWith("blob:")) URL.revokeObjectURL(previewUrl);
  previewUrl = null;
};

connect();
