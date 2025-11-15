const express = require("express");
const WebSocket = require("ws");
const http = require("http");
const { EventEmitter } = require("events");

const SECRET_KEY = process.env.MY_SECRET_KEY || "123456";
const DEFAULT_STREAMING_MODE = "real";

const log = (level, msg) => console[level](`[${level.toUpperCase()}] ${new Date().toISOString()} - ${msg}`);
const genId = () => `${Date.now()}_${Math.random().toString(36).slice(2, 11)}`;

class Queue {
  constructor() {
    this.msgs = [];
    this.waiters = [];
    this.closed = false;
  }
  push(msg) {
    if (this.closed) return;
    this.waiters.length ? this.waiters.shift().resolve(msg) : this.msgs.push(msg);
  }
  async pop() {
    if (this.closed) throw new Error("Queue closed");
    if (this.msgs.length) return this.msgs.shift();
    return new Promise((resolve, reject) => this.waiters.push({ resolve, reject }));
  }
  close() {
    this.closed = true;
    this.waiters.forEach((w) => w.reject(new Error("Queue closed")));
    this.waiters = [];
  }
}

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
    if (!this.heartbeat) this._startHeartbeat();
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
    }, 30000);
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
      if (!queue) return;
      const eventType = msg.event_type;
      if (eventType === "stream_close") queue.push({ type: "STREAM_END" });
      else if (["response_headers", "chunk", "error"].includes(eventType)) queue.push(msg);
    } catch (err) {
      log("error", `解析消息失败: ${err.message}`);
    }
  }
  hasConn = () => this.conns.size > 0;
  getConn = () => Array.from(this.conns).find((ws) => ws.isAlive);
  createQueue(id) {
    const queue = new Queue();
    this.queues.set(id, queue);
    return queue;
  }
  removeQueue(id) {
    this.queues.get(id)?.close();
    this.queues.delete(id);
  }
}

class Handler {
  constructor(server, conns) {
    this.server = server;
    this.conns = conns;
  }

  async handle(req, res) {
    if (req.path === "/v1/models") return this._handleModels(req, res);
    if (!this._auth(req, res) || !this._checkConn(res)) return;

    const id = genId();
    const isOpenAI = req.path.includes("/v1/chat/completions");
    const queue = this.conns.createQueue(id);
    try {
      const proxyReq = this._buildReq(req, id, isOpenAI);
      await this._dispatch(req, res, proxyReq, queue, isOpenAI);
    } catch (err) {
      this._error(err, res, isOpenAI);
    } finally {
      this.conns.removeQueue(id);
    }
  }

  async _handleModels(req, res) {
    log("info", "模型列表请求");
    if (!this._checkConn(res)) return;
    const id = genId();
    const proxyReq = {
      path: "/v1beta/models", method: "GET", headers: {}, query_params: {}, body: "",
      request_id: id, streaming_mode: "real", is_openai: false,
    };
    const queue = this.conns.createQueue(id);
    try {
      this._forward(proxyReq);
      const header = await queue.pop();
      if (header.event_type === "error") return this._send(res, header.status, header.message, true);
      const data = await queue.pop();
      await queue.pop();
      if (data.data) {
        const geminiModels = JSON.parse(data.data);
        res.status(200).json(this._convertModelsToOpenAI(geminiModels));
      } else {
        this._send(res, 500, "无法获取模型列表", true);
      }
    } catch (err) {
      this._error(err, res, true);
    } finally {
      this.conns.removeQueue(id);
    }
  }

  _convertModelsToOpenAI(geminiData) {
    return {
      object: "list",
      data: (geminiData.models || []).map(model => ({
        id: model.name.replace("models/", ""),
        object: "model",
        created: Math.floor(new Date(model.updateTime || Date.now()).getTime() / 1000),
        owned_by: "google",
      })),
    };
  }

  _auth(req, res) {
    const key = req.query.key || req.headers.authorization?.substring(7);
    if (key === SECRET_KEY) {
      delete req.query.key;
      return true;
    }
    this._send(res, 401, "Unauthorized", true);
    return false;
  }

  _checkConn(res) {
    if (this.conns.hasConn()) return true;
    this._send(res, 503, "无可用连接", true);
    return false;
  }

  _buildReq(req, id, isOpenAI) {
    const convertedBody = isOpenAI ? this._convertOpenAIToGemini(req.body) : req.body;
    const body = Buffer.isBuffer(convertedBody) ? convertedBody.toString("utf-8")
               : typeof convertedBody === "object" ? JSON.stringify(convertedBody)
               : convertedBody;
    return {
      path: isOpenAI ? this._convertOpenAIPath(req) : req.path,
      method: req.method,
      headers: req.headers,
      query_params: isOpenAI ? this._convertOpenAIQuery(req) : req.query,
      body,
      request_id: id,
      streaming_mode: this.server.mode,
      is_openai: isOpenAI,
    };
  }

  _convertOpenAIPath = (req) => {
    const model = req.body?.model || "gemini-2.0-flash-exp";
    const action = req.body?.stream ? "streamGenerateContent" : "generateContent";
    return `/v1beta/models/${model}:${action}`;
  }

  _convertOpenAIQuery = (req) => {
    const query = { ...req.query };
    delete query.key;
    if (req.body?.stream) query.alt = "sse";
    return query;
  }

  _convertOpenAIToGemini(openaiBody) {
    const { messages = [] } = openaiBody;
    const geminiBody = { contents: [] };
    const systemParts = [];
    messages.forEach(msg => {
      if (msg.role === "system") {
        const text = typeof msg.content === "string" ? msg.content : msg.content.find(c => c.type === "text")?.text || "";
        systemParts.push({ text });
      } else {
        const parts = [];
        if (typeof msg.content === "string") {
          parts.push({ text: msg.content });
        } else if (Array.isArray(msg.content)) {
          msg.content.forEach(item => {
            if (item.type === "text") parts.push({ text: item.text });
            else if (item.type === "image_url" && item.image_url.url.startsWith("data:")) {
              const match = item.image_url.url.match(/^data:image\/(\w+);base64,(.+)$/);
              if (match) parts.push({ inlineData: { mimeType: `image/${match[1]}`, data: match[2] } });
            }
          });
        }
        if (parts.length > 0) geminiBody.contents.push({ role: msg.role === "assistant" ? "model" : "user", parts });
      }
    });
    if (systemParts.length > 0) geminiBody.systemInstruction = { parts: systemParts };
    const genConfig = {};
    if (openaiBody.temperature !== undefined) genConfig.temperature = openaiBody.temperature;
    if (openaiBody.max_tokens !== undefined) genConfig.maxOutputTokens = openaiBody.max_tokens;
    if (openaiBody.top_p !== undefined) genConfig.topP = openaiBody.top_p;
    if (openaiBody.top_k !== undefined) genConfig.topK = openaiBody.top_k;
    if (openaiBody.stop) genConfig.stopSequences = Array.isArray(openaiBody.stop) ? openaiBody.stop : [openaiBody.stop];
    if (openaiBody.thinking_budget > 0) genConfig.thinkingConfig = { thoughtGenerationTokenBudget: Math.floor(openaiBody.thinking_budget) };
    if (Object.keys(genConfig).length > 0) geminiBody.generationConfig = genConfig;
    return geminiBody;
  }

  async _dispatch(req, res, proxyReq, queue, isOpenAI) {
    const stream = isOpenAI ? req.body?.stream : req.path.includes("streamGenerateContent");
    if (this.server.mode === "fake") {
      return stream ? this._fakeStream(req, res, proxyReq, queue, isOpenAI) : this._fakeNonStream(res, proxyReq, queue, isOpenAI);
    }
    return this._realStream(res, proxyReq, queue, stream, isOpenAI);
  }

  async _fakeNonStream(res, proxyReq, queue, isOpenAI) {
    this._forward(proxyReq);
    const header = await queue.pop();
    if (header.event_type === "error") return this._send(res, header.status, header.message, isOpenAI);
    this._setHeaders(res, header);
    const data = await queue.pop();
    await queue.pop();
    if (data.data) {
      if (isOpenAI) res.json(JSON.parse(this._convertGeminiToOpenAI(data.data, false)));
      else res.send(data.data);
    }
  }

  async _fakeStream(req, res, proxyReq, queue, isOpenAI) {
    this._sseHeaders(res);
    const timer = setInterval(() => res.write(this._keepAlive(req.path, isOpenAI)), 1000);
    try {
      this._forward(proxyReq);
      const header = await queue.pop();
      if (header.event_type === "error") throw new Error(header.message);
      const data = await queue.pop();
      await queue.pop();
      if (data.data) {
        if (isOpenAI) {
          res.write(this._convertGeminiToOpenAI(data.data, true));
          res.write("data: [DONE]\n\n");
        } else {
          res.write(`data: ${data.data}\n\n`);
        }
      }
    } catch (err) {
      this._sseError(res, err.message);
    } finally {
      clearInterval(timer);
      if (!res.writableEnded) res.end();
    }
  }

  async _realStream(res, proxyReq, queue, isStreamReq, isOpenAI) {
    this._forward(proxyReq);
    const header = await queue.pop();
    if (header.event_type === "error") return this._send(res, header.status, header.message, isOpenAI);
    if (isStreamReq && !header.headers?.["content-type"]) {
      header.headers = header.headers || {};
      header.headers["content-type"] = "text/event-stream";
    }
    this._setHeaders(res, header);
    let fullResponse = "";
    try {
      while (true) {
        const data = await queue.pop();
        if (data.type === "STREAM_END") break;
        if (!data.data) continue;
        if (isOpenAI && isStreamReq) res.write(this._convertGeminiSSEToOpenAI(data.data));
        else if (isOpenAI && !isStreamReq) fullResponse += data.data;
        else res.write(data.data);
      }
      if (isOpenAI && isStreamReq) res.write("data: [DONE]\n\n");
      else if (isOpenAI && !isStreamReq) res.json(JSON.parse(this._convertGeminiToOpenAI(fullResponse, false)));
    } finally {
      if (!res.writableEnded) res.end();
    }
  }

  _parseGeminiCandidate(geminiData) {
    try {
      const gemini = typeof geminiData === "string" ? JSON.parse(geminiData) : geminiData;
      const candidate = gemini.candidates?.[0];
      if (!candidate) return { text: "", finishReason: null };
      const text = candidate.content?.parts?.map(p => p.text || "").join("") || "";
      return { text, finishReason: candidate.finishReason };
    } catch (err) {
      log('error', `解析Gemini数据失败: ${err.message}`);
      return { text: "", finishReason: null };
    }
  }

  _convertGeminiToOpenAI(geminiData, isStream) {
    const { text, finishReason } = this._parseGeminiCandidate(geminiData);
    const finish = finishReason === "STOP" ? "stop" : null;
    if (isStream) {
      const chunk = {
        id: `chatcmpl-${genId()}`, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model: "gpt-4",
        choices: [{ index: 0, delta: text ? { content: text } : {}, finish_reason: finish }],
      };
      return `data: ${JSON.stringify(chunk)}\n\n`;
    } else {
      const response = {
        id: `chatcmpl-${genId()}`, object: "chat.completion", created: Math.floor(Date.now() / 1000), model: "gpt-4",
        choices: [{ index: 0, message: { role: "assistant", content: text }, finish_reason: finish || "length" }],
      };
      return JSON.stringify(response);
    }
  }

  _convertGeminiSSEToOpenAI(sseData) {
    return sseData.split("\n")
      .filter(line => line.startsWith("data: "))
      .map(line => this._convertGeminiToOpenAI(line.slice(6), true))
      .join("");
  }

  _forward = (proxyReq) => this.conns.getConn()?.send(JSON.stringify(proxyReq));
  _setHeaders = (res, header) => {
    res.status(header.status || 200);
    Object.entries(header.headers || {}).forEach(([k, v]) => {
      if (k.toLowerCase() !== "content-length") res.set(k, v);
    });
  }
  _sseHeaders = (res) => res.status(200).set({ "Content-Type": "text/event-stream", "Cache-Control": "no-cache", Connection: "keep-alive" });
  _keepAlive = (path, isOpenAI) => {
    const payload = isOpenAI ? {
      id: `chatcmpl-${genId()}`, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model: "gpt-4",
      choices: [{ index: 0, delta: {}, finish_reason: null }],
    } : {};
    return `data: ${JSON.stringify(payload)}\n\n`;
  }
  _sseError = (res, msg) => {
    if (res.writableEnded) return;
    res.write(`data: ${JSON.stringify({ error: { message: `[代理] ${msg}`, type: "proxy_error" } })}\n\n`);
  }
  _error(err, res, isOpenAI) {
    log("error", `错误: ${err.message}`);
    if (res.headersSent) {
      if (this.server.mode === "fake") this._sseError(res, err.message);
      if (!res.writableEnded) res.end();
    } else {
      this._send(res, 500, `代理错误: ${err.message}`, isOpenAI);
    }
  }
  _send(res, status, msg, isOpenAI) {
    if (res.headersSent) return;
    if (isOpenAI && status >= 400) {
      res.status(status).json({ error: { message: msg, type: "proxy_error" } });
    } else {
      res.status(status).type("text/plain").send(msg);
    }
  }
}

class Server extends EventEmitter {
  constructor() {
    super();
    this.mode = DEFAULT_STREAMING_MODE;
    this.conns = new Connections();
    this.handler = new Handler(this, this.conns);
  }
  async start() {
    const app = express();
    app.use(express.json({ limit: "100mb" }));
    app.get("/admin/set-mode", (req, res) => {
      if (["fake", "real"].includes(req.query.mode)) {
        this.mode = req.query.mode;
        log("info", `模式切换: ${this.mode}`);
        return res.send(`模式已切换: ${this.mode}`);
      }
      res.status(400).send('无效模式');
    });
    app.get("/admin/get-mode", (_, res) => res.send(`当前模式: ${this.mode}`));
    app.all(/(.*)/, (req, res) => {
      if (req.path === "/") return res.status(this.conns.hasConn() ? 200 : 404).send(this.conns.hasConn() ? "✅ 代理就绪" : "❌ 无连接");
      if (req.path.startsWith("/admin/") || req.path === "/favicon.ico") return res.status(204).send();
      this.handler.handle(req, res);
    });
    const httpServer = http.createServer(app);
    const wss = new WebSocket.Server({ server: httpServer });
    wss.on("connection", (ws, req) => this.conns.add(ws, { address: req.socket.remoteAddress }));
    return new Promise((resolve) => {
      httpServer.listen(process.env.PORT || 7860, "0.0.0.0", () => {
        log("info", `HTTP/WS 服务已启动: http://0.0.0.0:${process.env.PORT || 7860}`);
        log("info", `模式: ${this.mode}`);
        resolve();
      });
    });
  }
}

async function main() {
  try {
    await new Server().start();
  } catch (err) {
    console.error("启动失败:", err.message);
    process.exit(1);
  }
}

if (require.main === module) main();

module.exports = { Server, main };