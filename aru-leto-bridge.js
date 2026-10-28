#!/usr/bin/env node

// aru-leto-bridge.js — Leto → Aru 唤醒桥 v2.3
// 轮询 Leto 的 leto_list_pending，发现有人叫陆屿时，
// 通过 wake-send.mjs（与定时唤醒桥同一个发送器）投递唤醒。
// 只投递，不碰 presence；按 invocation_id 去重；3 分钟节流。
// v2.3：修复 leto 流式(SSE)响应不关连接导致的 poll timeout——
//       不再等待连接 end，读到完整 JSON-RPC 响应立即返回。

const fs = require("fs");
const http = require("http");
const https = require("https");
const { spawn } = require("child_process");

// 配置
const CONFIG_PATH = "/home/admin/aru-leto-bridge.json";
const STATE_PATH = "/home/admin/aru-leto-bridge-state.json";
const LOG_PATH = "/home/admin/aru-leto-bridge.log";

// 与 aru-wake-bridge-v2.js 相同的发送器和凭证包
const WAKE_SEND = "/home/admin/aru-host-source-0.33/wake-send.mjs";
const BUNDLE = "/home/admin/aru-credentials.json";

const POLL_INTERVAL_MS = 30 * 1000;
const MIN_WAKE_INTERVAL_MS = 3 * 60 * 1000;
const MAX_SEEN_IDS = 500;

// 工具
function log(msg) {
  const line = "[" + new Date().toISOString() + "] " + msg + "\n";
  try { fs.appendFileSync(LOG_PATH, line); } catch (_) {}
  process.stdout.write(line);
}

function loadJSON(p, fallback) {
  try { return JSON.parse(fs.readFileSync(p, "utf8")); } catch (_) { return fallback; }
}
function saveJSON(p, obj) {
  const tmp = p + ".tmp";
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 2));
  fs.renameSync(tmp, p);
}

// 从已收到的原始文本里解析出完整 JSON-RPC 响应。
// 兼容两种形态：
//   1) 普通 JSON 响应（application/json）
//   2) SSE 流里的 data: 帧（text/event-stream，可能发完不关连接）
// 一旦能解析出带 jsonrpc 字段的对象即视为完整。
function extractMcpResponse(raw) {
  const t = (raw || "").trim();
  try {
    const o = JSON.parse(t);
    if (o && typeof o === "object" && o.jsonrpc) return o;
  } catch (_) {}
  const lines = t.split(/\r?\n/);
  const parts = [];
  let sawData = false;
  for (const line of lines) {
    if (line.startsWith("data:")) { sawData = true; parts.push(line.slice(5).trim()); }
  }
  if (sawData) {
    try {
      const o = JSON.parse(parts.join(""));
      if (o && typeof o === "object" && o.jsonrpc) return o;
    } catch (_) {}
  }
  return null;
}

function request(urlStr, { method = "GET", headers = {}, body = null, timeoutMs = 20000 } = {}) {
  return new Promise((resolve, reject) => {
    const u = new URL(urlStr);
    const lib = u.protocol === "https:" ? https : http;
    let settled = false;
    const req = lib.request(
      {
        method,
        hostname: u.hostname,
        port: u.port || (u.protocol === "https:" ? 443 : 80),
        path: u.pathname + u.search,
        headers,
        timeout: timeoutMs,
      },
      (res) => {
        let data = "";
        res.setEncoding("utf8");
        res.on("data", (c) => {
          data += c;
          // 读到完整 JSON-RPC 响应就立即收工，不等连接关闭
          if (!settled && extractMcpResponse(data)) {
            settled = true;
            req.destroy();
            resolve({ status: res.statusCode, body: data });
          }
        });
        res.on("end", () => {
          if (!settled) { settled = true; resolve({ status: res.statusCode, body: data }); }
        });
        res.on("error", (e) => { if (!settled) { settled = true; reject(e); } });
      }
    );
    req.on("timeout", () => {
      if (!settled) { settled = true; req.destroy(); reject(new Error("timeout")); }
    });
    req.on("error", (e) => { if (!settled) { settled = true; reject(e); } });
    if (body) req.write(body);
    req.end();
  });
}

async function mcpCall(cfg, toolName, args) {
  const payload = JSON.stringify({
    jsonrpc: "2.0",
    id: Date.now(),
    method: "tools/call",
    params: { name: toolName, arguments: args || {} },
  });
  const headers = {
    "Content-Type": "application/json",
    Accept: "application/json, text/event-stream",
  };
  if (cfg.letoToken) headers["Authorization"] = "Bearer " + cfg.letoToken;

  const res = await request(cfg.letoMcpUrl, { method: "POST", headers, body: payload });
  if (res.status < 200 || res.status >= 300) {
    throw new Error("MCP HTTP " + res.status + ": " + res.body.slice(0, 200));
  }
  const json = extractMcpResponse(res.body);
  if (!json) throw new Error("MCP empty response");
  if (json.error) throw new Error("MCP error: " + JSON.stringify(json.error).slice(0, 200));
  return json.result;
}

// 投递唤醒：与 aru-wake-bridge-v2.js 完全同款
// 明文从 stdin 喂给 wake-send.mjs，密封和投递由它完成，exit=0 即成功
function submitWake(text) {
  return new Promise((resolve) => {
    const child = spawn("node", [WAKE_SEND, "--bundle", BUNDLE], {
      stdio: ["pipe", "pipe", "pipe"],
    });
    let out = "";
    child.stdout.on("data", (d) => (out += d));
    child.stderr.on("data", (d) => (out += d));
    child.on("close", (code) => resolve({ ok: code === 0, out: out.trim() }));
    child.on("error", (e) => resolve({ ok: false, out: "spawn error: " + e.message }));
    child.stdin.write(text);
    child.stdin.end();
  });
}

function buildWakeText(inv) {
  return (
    "[Leto] 群里有人叫陆屿（invocation: " + inv.id + "）\n\n" +
    (inv.summary || JSON.stringify(inv.raw).slice(0, 800)) +
    "\n\n回应步骤：\n" +
    "1. leto_get_invocation 拿到这次调用的完整上下文\n" +
    "2. leto_reply 带上 invocation_id 回话\n" +
    "3. 回完别立刻睡，用 leto_continue 跟进这段对话（传 next_after_message_id），" +
    "聊到对方停了再撤。不想聊用 leto_decline。"
  );
}

// 提取 invocation
function extractInvocations(result) {
  let data = null;
  try {
    const t = result && result.content && result.content[0] && result.content[0].text;
    if (t) data = JSON.parse(t);
  } catch (_) {}
  if (!data) data = result;

  const lists = [];
  for (const key of ["invocations", "pending", "items", "calls"]) {
    if (Array.isArray(data && data[key])) lists.push(...data[key]);
  }
  if (Array.isArray(data)) lists.push(...data);

  return lists
    .map((it) => ({
      raw: it,
      id: it.invocation_id || it.invocationId || it.id || null,
      title: it.title || it.kind || it.type || null,
      summary: it.summary || it.text || it.preview || null,
    }))
    .filter((it) => it.id);
}

// 主循环
async function tick(cfg, state) {
  let result;
  try {
    result = await mcpCall(cfg, "leto_list_pending", {});
  } catch (e) {
    log("poll error: " + e.message);
    return;
  }

  const invocations = extractInvocations(result);
  if (!invocations.length) return;

  const now = Date.now();
  for (const inv of invocations) {
    if (state.seenIds.includes(inv.id)) continue;

    const sinceLast = now - (state.lastWakeAt || 0);
    if (sinceLast < MIN_WAKE_INTERVAL_MS) {
      log("throttled, mark seen: " + inv.id);
      state.seenIds.push(inv.id);
      continue;
    }

    const res = await submitWake(buildWakeText(inv));
    if (res.ok) {
      state.lastWakeAt = now;
      state.seenIds.push(inv.id);
      log("wake submitted OK for " + inv.id + " [" + (inv.title || "?") + "] " + res.out);
    } else {
      state.seenIds.push(inv.id);
      log("wake submit failed for " + inv.id + ": " + res.out.slice(0, 200));
    }
  }

  if (state.seenIds.length > MAX_SEEN_IDS) {
    state.seenIds = state.seenIds.slice(-MAX_SEEN_IDS);
  }
}

async function main() {
  const cfg = loadJSON(CONFIG_PATH, null);
  if (!cfg || !cfg.letoMcpUrl) {
    log("FATAL: config missing. Need letoMcpUrl in " + CONFIG_PATH);
    process.exit(1);
  }
  log("aru-leto-bridge v2.3 已启动，轮询 " + POLL_INTERVAL_MS / 1000 + "s，最小唤醒间隔 " + MIN_WAKE_INTERVAL_MS / 60000 + "min");
  log("leto MCP: " + cfg.letoMcpUrl);
  log("wake-send: " + WAKE_SEND);

  const state = loadJSON(STATE_PATH, { seenIds: [], lastWakeAt: 0 });

  try { await tick(cfg, state); saveJSON(STATE_PATH, state); }
  catch (e) { log("tick error: " + e.message); }

  setInterval(async () => {
    try { await tick(cfg, state); saveJSON(STATE_PATH, state); }
    catch (e) { log("tick error: " + e.message); }
  }, POLL_INTERVAL_MS);

  process.stdin.resume();
  log("事件循环已挂起，桥持续运行中");
}

main();
