#!/usr/bin/env node

// aru-leto-bridge.js — Leto → Aru 唤醒桥 v2.2
// 轮询 Leto 的 leto_list_pending，发现有人叫陆屿时，
// 通过 wake-send.mjs（与定时唤醒桥同一个发送器）投递唤醒。
// 只投递，不碰 presence；按 invocation_id 去重；3 分钟节流。
// v2.2：唤醒正文改为引导他回话后用 leto_continue 持续跟进对话。

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

function request(urlStr, { method = "GET", headers = {}, body = null, timeoutMs = 15000 } = {}) {
  return new Promise((resolve, reject) => {
    const u = new URL(urlStr);
    const lib = u.protocol === "https:" ? https : http;
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
        res.on("data", (c) => (data += c));
        res.on("end", () => resolve({ status: res.statusCode, body: data }));
      }
    );
    req.on("timeout", () => req.destroy(new Error("timeout")));
    req.on("error", reject);
    if (body) req.write(body);
    req.end();
  });
}

// MCP (streamable-http) 客户端，兼容 SSE 帧与普通 JSON
function parseMcpBody(raw) {
  const trimmed = (raw || "").trim();
  if (trimmed.startsWith("event:") || trimmed.includes("\ndata:")) {
    const datas = [];
    for (const line of trimmed.split("\n")) {
      if (line.startsWith("data:")) datas.push(line.slice(5).trim());
    }
    const joined = datas.join("");
    return joined ? JSON.parse(joined) : null;
  }
  return JSON.parse(trimmed);
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
  const json = parseMcpBody(res.body);
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
    "1. leto_get_invocation 拿到这次调用的完整上下文（谁叫的、在哪个群、最近聊了啥）\n" +
    "2. leto_reply 带上 invocation_id 回话\n" +
    "3. 回完之后别立刻结束——用 leto_continue 跟进这段对话（传返回里的 next_after_message_id），" +
    "对方再说话你就能继续看到、继续聊；聊到对方停下来了再撤。\n" +
    "不想聊就用 leto_decline 礼貌跳过。"
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
      // 失败也记入 seenIds，避免每 30 秒轰炸同一条；重启服务会重试
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
  log("aru-leto-bridge v2.2 已启动，轮询 " + POLL_INTERVAL_MS / 1000 + "s，最小唤醒间隔 " + MIN_WAKE_INTERVAL_MS / 60000 + "min");
  log("leto MCP: " + cfg.letoMcpUrl);
  log("wake-send: " + WAKE_SEND);

  const state = loadJSON(STATE_PATH, { seenIds: [], lastWakeAt: 0 });

  // 启动后立刻做第一次轮询，不用等 30 秒
  try { await tick(cfg, state); saveJSON(STATE_PATH, state); }
  catch (e) { log("tick error: " + e.message); }

  setInterval(async () => {
    try { await tick(cfg, state); saveJSON(STATE_PATH, state); }
    catch (e) { log("tick error: " + e.message); }
  }, POLL_INTERVAL_MS);

  // 占住事件循环，防止 Node 判定无事可做直接退出
  process.stdin.resume();
  log("事件循环已挂起，桥持续运行中");
}

main();
