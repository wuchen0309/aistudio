const express = require("express");
const WebSocket = require("ws");
const http = require("http");
const { EventEmitter } = require("events");

// ═══════════════════════════ 配置 ═══════════════════════════
const CONFIG = {
  SECRET_KEY: process.env.MY_SECRET_KEY || "123456",
  PORT: process.env.PORT || 7860,
  HOST: "0.0.0.0",
  STREAMING_MODE: "real",
  WS_HEARTBEAT: 30000,
  QUEUE_TIMEOUT: 600000,
  MAX_RETRIES: 3,
  RETRY_DELAY: 2000,
};

// ═══════════════════════════ 工具 ═══════════════════════════
const log = (level, msg) =>
  console[level](`[${level.toUpperCase()}] ${new Date().toISOString()} - ${msg}`);

const genId = () => `${Date.now()}_${Math.random().toString(36).slice(2, 11)}`;

const isStream = (path) => path.includes("streamGenerateContent");

// ═══════════════════════════ 消息队列 ═══════════════════════════
class Queue extends EventEmitter {
  constructor(timeout = CONFIG.QUEUE_TIMEOUT) {
    super();
    this.msgs = [];
    this.waiters = [];
    this.timeout = timeout;
    this.closed = false;
  }

  push(msg) {
    if (this.closed) return;
    const waiter = this.waiters.shift();
    waiter ? this._resolve(waiter, msg) : this.msgs.push(msg);
  }

  async pop(timeout = this.timeout) {
    if (this.closed) throw new Error("Queue closed");
    if (this.msgs.length) return this.msgs.shift();

    return new Promise((resolve, reject) => {
      const waiter = { resolve, reject, timer: null };
      this.waiters.push(waiter);
      waiter.timer = setTimeout(() => {
        const idx = this.waiters.indexOf(waiter);
        if (idx !== -1) {
          this.waiters.splice(idx, 1);
          reject(new Error("Queue timeout"));
        }
      }, timeout);
    });
  }

  close() {
    this.closed = true;
    this.waiters.forEach((w) => {
      clearTimeout(w.timer);
      w.reject(new Error("Queue closed"));
    });
    this.waiters = [];
    this.msgs = [];
  }

  _resolve(waiter, msg) {
    clearTimeout(waiter.timer);
    waiter.resolve(msg);
  }
}

// ═══════════════════════════ 连接管理 ═══════════════════════════
class Connections extends EventEmitter {
  constructor() {
    super();
    this.conns = new Set();
    this.queues = new Map();
    this.heartbeat = null;
  }

  add(ws, info) {
    ws.isAlive = true;
    this.conns.add(ws);
    log("info", `客户端连接: ${info.address}`);

    ws.on("pong", () => (ws.isAlive = true));
    ws.on("message", (data) => this._onMessage(data.toString()));
    ws.on("close", () => this._remove(ws));
    ws.on("error", (err) => log("error", `WS错误: ${err.message}`));

    if (this.conns.size === 1) this._startHeartbeat();
    this.emit("add", ws);
  }

  _startHeartbeat() {
    log("info", "心跳启动");
    this.heartbeat = setInterval(() => {
      this.conns.forEach((ws) => {
        if (!ws.isAlive) return ws.terminate();
        ws.isAlive = false;
        ws.ping();
      });
    }, CONFIG.WS_HEARTBEAT);
  }

  _remove(ws) {
    this.conns.delete(ws);
    log("info", "客户端断开");
    this.queues.forEach((q) => q.close());
    this.queues.clear();
    if (!this.conns.size) this._stopHeartbeat();
    this.emit("remove", ws);
  }

  _stopHeartbeat() {
    if (!this.heartbeat) return;
    clearInterval(this.heartbeat);
    this.heartbeat = null;
    log("info", "心跳停止");
  }

  _onMessage(data) {
    try {
      const msg = JSON.parse(data);
      const queue = this.queues.get(msg.request_id);
      if (!queue) return log("warn", `未知请求ID: ${msg.request_id}`);

      if (msg.event_type === "stream_close") {
        queue.push({ type: "STREAM_END" });
      } else if (["response_headers", "chunk", "error"].includes(msg.event_type)) {
        queue.push(msg);
      }
    } catch (err) {
      log("error", `解析消息失败: ${err.message}`);
    }
  }

  hasConn() {
    return this.conns.size > 0;
  }

  getConn() {
    return Array.from(this.conns).find((ws) => ws.isAlive);
  }

  createQueue(id) {
    const queue = new Queue();
    this.queues.set(id, queue);
    return queue;
  }

  removeQueue(id) {
    const queue = this.queues.get(id);
    if (queue) {
      queue.close();
      this.queues.delete(id);
    }
  }
}

// ═══════════════════════════ 请求处理 ═══════════════════════════
class Handler {
  constructor(server, conns) {
    this.server = server;
    this.conns = conns;
  }

  async handle(req, res) {
    if (!this._auth(req, res)) return;
    if (!this._checkConn(res)) return;

    const id = genId();
    const proxyReq = this._buildReq(req, id);
    const queue = this.conns.createQueue(id);

    try {
      await this._dispatch(req, res, proxyReq, queue);
    } catch (err) {
      this._error(err, res);
    } finally {
      this.conns.removeQueue(id);
    }
  }

  _auth(req, res) {
    const key = req.query.key;
    if (key === CONFIG.SECRET_KEY) {
      delete req.query.key;
      log("info", `验证通过: ${req.method} ${req.path}`);
      return true;
    }
    log("warn", `验证失败: ${req.url}`);
    this._send(res, 401, "Unauthorized");
    return false;
  }

  _checkConn(res) {
    if (this.conns.hasConn()) return true;
    this._send(res, 503, "无可用连接");
    return false;
  }

  _buildReq(req, id) {
    let body = "";
    if (Buffer.isBuffer(req.body)) body = req.body.toString("utf-8");
    else if (typeof req.body === "string") body = req.body;
    else if (req.body) body = JSON.stringify(req.body);

    return {
      path: req.path,
      method: req.method,
      headers: req.headers,
      query_params: req.query,
      body,
      request_id: id,
      streaming_mode: this.server.mode,
    };
  }

  async _dispatch(req, res, proxyReq, queue) {
    const mode = this.server.mode;
    const stream = isStream(req.path);

    if (mode === "fake") {
      return stream
        ? this._fakeStream(req, res, proxyReq, queue)
        : this._fakeNonStream(res, proxyReq, queue);
    }
    return this._realStream(res, proxyReq, queue, stream);
  }

  async _fakeNonStream(res, proxyReq, queue) {
    log("info", "非流式请求");
    this._forward(proxyReq);

    const header = await queue.pop();
    if (header.event_type === "error") {
      return this._send(res, header.status || 500, header.message);
    }

    this._setHeaders(res, header);
    const data = await queue.pop();
    await queue.pop(); // end

    if (data.data) res.send(data.data);
    log("info", "JSON响应完成");
  }

  async _fakeStream(req, res, proxyReq, queue) {
    this._sseHeaders(res);
    const keepAlive = this._keepAlive(req.path);
    const timer = setInterval(() => res.write(keepAlive), 1000);

    try {
      log("info", "假流式开始");
      this._forward(proxyReq);

      const header = await queue.pop();
      if (header.event_type === "error") {
        this._sseError(res, header.message);
        throw new Error(header.message);
      }

      const data = await queue.pop();
      await queue.pop(); // end

      if (data.data) {
        res.write(`data: ${data.data}\n\n`);
        log("info", "SSE响应完成");
      }
    } finally {
      clearInterval(timer);
      if (!res.writableEnded) res.end();
      log("info", "假流式结束");
    }
  }

  async _realStream(res, proxyReq, queue, isStreamReq) {
    const header = await this._fetchWithRetry(proxyReq, queue);
    if (header.event_type === "error") {
      return this._send(res, header.status, header.message);
    }

    if (isStreamReq && !header.headers?.["content-type"]) {
      header.headers = header.headers || {};
      header.headers["content-type"] = "text/event-stream";
    }

    this._setHeaders(res, header);
    log("info", "真流式开始");

    try {
      while (true) {
        const data = await queue.pop(30000);
        if (data.type === "STREAM_END") break;
        if (data.data) res.write(data.data);
      }
    } catch (err) {
      if (err.message !== "Queue timeout") throw err;
      log("warn", "真流式超时");
    } finally {
      if (!res.writableEnded) res.end();
      log("info", "真流式结束");
    }
  }

  async _fetchWithRetry(proxyReq, queue) {
    for (let i = 1; i <= CONFIG.MAX_RETRIES; i++) {
      log("info", `请求尝试 ${i}/${CONFIG.MAX_RETRIES}`);
      this._forward(proxyReq);
      const header = await queue.pop();

      const isError =
        header.event_type === "error" &&
        header.status >= 400 &&
        header.status <= 599;

      if (!isError || i === CONFIG.MAX_RETRIES) return header;

      log("warn", `收到${header.status}错误，${CONFIG.RETRY_DELAY / 1000}秒后重试`);
      await new Promise((r) => setTimeout(r, CONFIG.RETRY_DELAY));
    }
  }

  _forward(proxyReq) {
    const conn = this.conns.getConn();
    if (!conn) throw new Error("无可用连接");
    conn.send(JSON.stringify(proxyReq));
  }

  _setHeaders(res, header) {
    res.status(header.status || 200);
    Object.entries(header.headers || {}).forEach(([k, v]) => {
      if (k.toLowerCase() !== "content-length") res.set(k, v);
    });
  }

  _sseHeaders(res) {
    res.status(200).set({
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    });
  }

  _keepAlive(path) {
    if (path.includes("chat/completions")) {
      return `data: ${JSON.stringify({
        id: `chatcmpl-${genId()}`,
        object: "chat.completion.chunk",
        created: Math.floor(Date.now() / 1000),
        model: "gpt-4",
        choices: [{ index: 0, delta: {}, finish_reason: null }],
      })}\n\n`;
    }
    if (path.includes("generateContent") || path.includes("streamGenerateContent")) {
      return `data: ${JSON.stringify({
        candidates: [
          {
            content: { parts: [{ text: "" }], role: "model" },
            finishReason: null,
            index: 0,
            safetyRatings: [],
          },
        ],
      })}\n\n`;
    }
    return "data: {}\n\n";
  }

  _sseError(res, msg) {
    if (res.writableEnded) return;
    res.write(
      `data: ${JSON.stringify({
        error: { message: `[代理] ${msg}`, type: "proxy_error", code: "proxy_error" },
      })}\n\n`
    );
  }

  _error(err, res) {
    if (res.headersSent) {
      log("error", `错误(头已发): ${err.message}`);
      if (this.server.mode === "fake") this._sseError(res, err.message);
      if (!res.writableEnded) res.end();
      return;
    }
    log("error", `错误: ${err.message}`);
    const status = err.message.includes("超时") ? 504 : 500;
    this._send(res, status, `代理错误: ${err.message}`);
  }

  _send(res, status, msg) {
    if (res.headersSent) return;
    res.status(status).type("text/plain").send(msg);
  }
}

// ═══════════════════════════ 服务器 ═══════════════════════════
class Server extends EventEmitter {
  constructor() {
    super();
    this.mode = CONFIG.STREAMING_MODE;
    this.conns = new Connections();
    this.handler = new Handler(this, this.conns);
    this.server = null;
    this.wss = null;
  }

  async start() {
    const app = express();
    app.use(express.json({ limit: "100mb" }));
    app.use(express.urlencoded({ extended: true, limit: "100mb" }));
    app.use(express.raw({ type: "*/*", limit: "100mb" }));

    // 管理路由
    app.get("/admin/set-mode", (req, res) => {
      const mode = req.query.mode;
      if (mode === "fake" || mode === "real") {
        this.mode = mode;
        log("info", `模式切换: ${mode}`);
        return res.send(`模式已切换: ${mode}`);
      }
      res.status(400).send('无效模式，使用 "fake" 或 "real"');
    });

    app.get("/admin/get-mode", (_, res) => res.send(`当前模式: ${this.mode}`));

    // 代理路由
    app.all(/(.*)/, (req, res, next) => {
      if (req.path === "/") {
        log("info", "根路径访问");
        return res
          .status(this.conns.hasConn() ? 200 : 404)
          .send(
            this.conns.hasConn()
              ? "✅ 浏览器已连接，代理就绪"
              : "❌ 无浏览器连接，请运行浏览器脚本"
          );
      }
      if (req.path.startsWith("/admin/")) return next();
      if (req.path === "/favicon.ico") return res.status(204).send();
      this.handler.handle(req, res);
    });

    const httpServer = http.createServer(app);
    this.server = httpServer;
    this.wss = new WebSocket.Server({ server: httpServer });

    this.wss.on("connection", (ws, req) => {
      this.conns.add(ws, { address: req.socket.remoteAddress });
    });

    return new Promise((resolve) => {
      httpServer.listen(CONFIG.PORT, CONFIG.HOST, () => {
        log("info", `HTTP: http://${CONFIG.HOST}:${CONFIG.PORT}`);
        log("info", `WS: ws://${CONFIG.HOST}:${CONFIG.PORT}`);
        log("info", `模式: ${this.mode}`);
        resolve();
      });
    });
  }
}

// ═══════════════════════════ 启动 ═══════════════════════════
async function main() {
  const server = new Server();
  try {
    await server.start();
  } catch (err) {
    console.error("启动失败:", err.message);
    process.exit(1);
  }
}

if (require.main === module) main();

module.exports = { Server, main };
