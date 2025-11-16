const express = require("express");
const WebSocket = require("ws");
const http = require("http");

const SECRET_KEY = process.env.MY_SECRET_KEY || "123456";
const DEFAULT_STREAMING_MODE = "real";

const log = (level, msg) => console[level](`[${level.toUpperCase()}] ${new Date().toISOString()} - ${msg}`);
const genId = () => `${Date.now()}_${Math.random().toString(36).slice(2, 11)}`;

class Queue {
  #msgs = [];
  #waiters = [];
  #closed = false;
  push(msg) {
    if (this.#closed) return;
    this.#waiters.length ? this.#waiters.shift().resolve(msg) : this.#msgs.push(msg);
  }
  async pop() {
    if (this.#closed) throw new Error("Queue closed");
    if (this.#msgs.length) return this.#msgs.shift();
    return new Promise((resolve, reject) => this.#waiters.push({ resolve, reject }));
  }
  close() {
    this.#closed = true;
    this.#waiters.forEach((w) => w.reject(new Error("Queue closed")));
    this.#waiters = [];
  }
}

class Connections {
  #conn = null;
  #queues = new Map();
  #heartbeat = null;

  add(ws, info) {
    if (this.#conn) {
      log("warn", "已有客户端连接，关闭旧连接并替换。");
      this.#conn.close(1000, "Replaced by new connection");
    }
    this.#conn = ws;
    ws.isAlive = true;
    log("info", `客户端连接: ${info.address}`);
    ws.on("pong", () => { ws.isAlive = true; });
    ws.on("message", (data) => this.#onMessage(data.toString()));
    ws.on("close", () => this.#onClose(ws));
    ws.on("error", (err) => log("error", `WS错误: ${err.message}`));
    if (!this.#heartbeat) this.#startHeartbeat();
  }

  #startHeartbeat() {
    log("info", "心跳启动");
    this.#heartbeat = setInterval(() => {
      if (!this.#conn) return;
      if (!this.#conn.isAlive) return this.#conn.terminate();
      this.#conn.isAlive = false;
      this.#conn.ping();
    }, 30000);
  }

  #onClose(ws) {
    if (this.#conn !== ws) return;
    this.#conn = null;
    log("info", "客户端断开");
    this.#queues.forEach((q) => q.close());
    this.#queues.clear();
    this.#stopHeartbeat();
  }

  #stopHeartbeat() {
    clearInterval(this.#heartbeat);
    this.#heartbeat = null;
    log("info", "心跳停止");
  }

  #onMessage(data) {
    try {
      const msg = JSON.parse(data);
      const queue = this.#queues.get(msg.request_id);
      if (!queue) return;
      if (msg.event_type === "stream_close") queue.push({ type: "STREAM_END" });
      else if (["response_headers", "chunk", "error"].includes(msg.event_type)) queue.push(msg);
    } catch (err) {
      log("error", `解析消息失败: ${err.message}`);
    }
  }

  hasConn = () => !!this.#conn;
  getConn = () => (this.#conn?.isAlive ? this.#conn : null);
  createQueue = (id) => this.#queues.set(id, new Queue()).get(id);
  removeQueue = (id) => this.#queues.get(id)?.close() && this.#queues.delete(id);
  forward = (proxyReq) => this.getConn()?.send(JSON.stringify(proxyReq));
}

class FormatConverter {
  toOpenAIModels(geminiData) {
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

  fromOpenAIRequest(req, id, streamingMode) {
    const { body: openaiBody } = req;
    const geminiBody = this.#convertOpenAIToGemini(openaiBody);
    return {
      path: this.#convertOpenAIPath(openaiBody),
      method: "POST",
      headers: req.headers,
      query_params: this.#convertOpenAIQuery(req),
      body: JSON.stringify(geminiBody),
      request_id: id,
      streaming_mode: streamingMode,
      is_openai: true,
    };
  }

  toOpenAIResponse(geminiData, isStream) {
    const { text, finishReason } = this.#parseGeminiCandidate(geminiData);
    const finish = finishReason === "STOP" ? "stop" : null;
    if (isStream) {
      const chunk = {
        id: `chatcmpl-${genId()}`, object: "chat.completion.chunk", created: Date.now() / 1000 | 0, model: "gpt-4",
        choices: [{ index: 0, delta: text ? { content: text } : {}, finish_reason: finish }],
      };
      return `data: ${JSON.stringify(chunk)}\n\n`;
    }
    return JSON.stringify({
      id: `chatcmpl-${genId()}`, object: "chat.completion", created: Date.now() / 1000 | 0, model: "gpt-4",
      choices: [{ index: 0, message: { role: "assistant", content: text }, finish_reason: finish || "length" }],
    });
  }

  toOpenAISSE(sseData) {
    return sseData.split("\n")
      .filter(line => line.startsWith("data: "))
      .map(line => this.toOpenAIResponse(line.slice(6), true))
      .join("");
  }
  
  #convertOpenAIPath = (body) => `/v1beta/models/${body?.model || "gemini-pro"}:${body?.stream ? "streamGenerateContent" : "generateContent"}`;
  
  #convertOpenAIQuery = (req) => {
    const query = { ...req.query };
    delete query.key;
    if (req.body?.stream) query.alt = "sse";
    return query;
  };

  #convertOpenAIToGemini(body) {
    const geminiBody = { contents: [] };
    const systemParts = [];
    (body.messages || []).forEach(msg => {
      if (msg.role === "system") {
        const text = typeof msg.content === "string" ? msg.content : (msg.content.find(c => c.type === "text")?.text || "");
        systemParts.push({ text });
      } else {
        const parts = [];
        if (typeof msg.content === "string") parts.push({ text: msg.content });
        else if (Array.isArray(msg.content)) {
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
    if (body.temperature !== undefined) genConfig.temperature = body.temperature;
    if (body.max_tokens !== undefined) genConfig.maxOutputTokens = body.max_tokens;
    if (body.top_p !== undefined) genConfig.topP = body.top_p;
    if (body.top_k !== undefined) genConfig.topK = body.top_k;
    if (body.stop) genConfig.stopSequences = Array.isArray(body.stop) ? body.stop : [body.stop];
    if (body.thinking_budget > 0) genConfig.thinkingConfig = { thoughtGenerationTokenBudget: Math.floor(body.thinking_budget) };
    if (Object.keys(genConfig).length > 0) geminiBody.generationConfig = genConfig;
    return geminiBody;
  }
  
  #parseGeminiCandidate(geminiData) {
    try {
      const gemini = typeof geminiData === "string" ? JSON.parse(geminiData) : geminiData;
      const candidate = gemini.candidates?.[0];
      if (!candidate) return { text: "", finishReason: null };
      const text = candidate.content?.parts?.map(p => p.text || "").join("") || "";
      return { text, finishReason: candidate.finishReason };
    } catch {
      return { text: "", finishReason: null };
    }
  }
}

class Handler {
  #server;
  #conns;
  #converter = new FormatConverter();

  constructor(server, conns) {
    this.#server = server;
    this.#conns = conns;
  }

  async handle(req, res, isOpenAI = false) {
    if (!this.#auth(req, res) || !this.#checkConn(res, isOpenAI)) return;
    const id = genId();
    const queue = this.#conns.createQueue(id);
    try {
      const proxyReq = isOpenAI
        ? this.#converter.fromOpenAIRequest(req, id, this.#server.mode)
        : this.#buildNativeReq(req, id);
      await this.#dispatch(req, res, proxyReq, queue, isOpenAI);
    } catch (err) {
      this.#error(err, res, isOpenAI);
    } finally {
      this.#conns.removeQueue(id);
    }
  }

  async handleModels(req, res) {
    log("info", "模型列表请求");
    if (!this.#checkConn(res, true)) return;
    const id = genId();
    const queue = this.#conns.createQueue(id);
    try {
      this.#conns.forward({ path: "/v1beta/models", method: "GET", request_id: id });
      const header = await queue.pop();
      if (header.event_type === "error") return this.#send(res, header.status, header.message, true);
      const data = await queue.pop();
      await queue.pop();
      if (data.data) res.json(this.#converter.toOpenAIModels(JSON.parse(data.data)));
      else this.#send(res, 500, "无法获取模型列表", true);
    } catch (err) {
      this.#error(err, res, true);
    } finally {
      this.#conns.removeQueue(id);
    }
  }

  #auth = (req, res) => {
    if ((req.query.key || req.headers.authorization?.substring(7)) === SECRET_KEY) return true;
    this.#send(res, 401, "Unauthorized", true);
    return false;
  }

  #checkConn = (res, isOpenAI) => {
    if (this.#conns.hasConn()) return true;
    this.#send(res, 503, "无可用连接", isOpenAI);
    return false;
  }

  #buildNativeReq(req, id) {
    const query = { ...req.query };
    delete query.key; // *** FIXED: Remove key from native requests as well
    const body = Buffer.isBuffer(req.body) ? req.body.toString("utf-8") : (typeof req.body === "object" ? JSON.stringify(req.body) : req.body);
    return { path: req.path, method: req.method, headers: req.headers, query_params: query, body, request_id: id, streaming_mode: this.#server.mode };
  }

  async #dispatch(req, res, proxyReq, queue, isOpenAI) {
    const isStream = isOpenAI ? req.body?.stream : req.path.includes("streamGenerateContent");
    if (this.#server.mode === "fake") {
      isStream ? await this.#fakeStream(req, res, proxyReq, queue, isOpenAI) : await this.#fakeNonStream(res, proxyReq, queue, isOpenAI);
    } else {
      await this.#realStream(res, proxyReq, queue, isStream, isOpenAI);
    }
  }

  async #fakeNonStream(res, proxyReq, queue, isOpenAI) {
    this.#conns.forward(proxyReq);
    const header = await queue.pop();
    if (header.event_type === "error") return this.#send(res, header.status, header.message, isOpenAI);
    this.#setHeaders(res, header);
    const data = await queue.pop();
    await queue.pop();
    if (data.data) isOpenAI ? res.json(JSON.parse(this.#converter.toOpenAIResponse(data.data, false))) : res.send(data.data);
  }

  async #fakeStream(req, res, proxyReq, queue, isOpenAI) {
    this.#sseHeaders(res);
    const timer = setInterval(() => res.write(this.#keepAlive(isOpenAI)), 1000);
    try {
      this.#conns.forward(proxyReq);
      const header = await queue.pop();
      if (header.event_type === "error") throw new Error(header.message);
      const data = await queue.pop();
      await queue.pop();
      if (data.data) {
        if (isOpenAI) {
          res.write(this.#converter.toOpenAIResponse(data.data, true));
          res.write("data: [DONE]\n\n");
        } else res.write(`data: ${data.data}\n\n`);
      }
    } catch (err) {
      this.#sseError(res, err.message);
    } finally {
      clearInterval(timer);
      if (!res.writableEnded) res.end();
    }
  }

  async #realStream(res, proxyReq, queue, isStream, isOpenAI) {
    this.#conns.forward(proxyReq);
    const header = await queue.pop();
    if (header.event_type === "error") return this.#send(res, header.status, header.message, isOpenAI);
    if (isStream && !header.headers?.["content-type"]) (header.headers ||= {})["content-type"] = "text/event-stream";
    this.#setHeaders(res, header);
    let fullResponse = "";
    try {
      while (true) {
        const data = await queue.pop();
        if (data.type === "STREAM_END") break;
        if (!data.data) continue;
        if (isOpenAI && isStream) res.write(this.#converter.toOpenAISSE(data.data));
        else if (isOpenAI && !isStream) fullResponse += data.data;
        else res.write(data.data);
      }
      if (isOpenAI && isStream) res.write("data: [DONE]\n\n");
      else if (isOpenAI && !isStream) res.json(JSON.parse(this.#converter.toOpenAIResponse(fullResponse, false)));
    } finally {
      if (!res.writableEnded) res.end();
    }
  }

  #setHeaders = (res, header) => {
    res.status(header.status || 200);
    Object.entries(header.headers || {}).forEach(([k, v]) => k.toLowerCase() !== "content-length" && res.set(k, v));
  }
  #sseHeaders = (res) => res.status(200).set({ "Content-Type": "text/event-stream", "Cache-Control": "no-cache", Connection: "keep-alive" });
  #keepAlive = (isOpenAI) => `data: ${isOpenAI ? JSON.stringify({ id: `cmpl-${genId()}`, choices: [{ delta: {} }] }) : "{}"}\n\n`;
  #sseError = (res, msg) => !res.writableEnded && res.write(`data: ${JSON.stringify({ error: { message: `[代理] ${msg}` } })}\n\n`);
  #error(err, res, isOpenAI) {
    log("error", `错误: ${err.message}`);
    if (res.headersSent) {
      if (this.#server.mode === "fake") this.#sseError(res, err.message);
      if (!res.writableEnded) res.end();
    } else this.#send(res, 500, `代理错误: ${err.message}`, isOpenAI);
  }
  #send(res, status, msg, isOpenAI) {
    if (res.headersSent) return;
    isOpenAI && status >= 400 ? res.status(status).json({ error: { message: msg } }) : res.status(status).type("text/plain").send(msg);
  }
}

class Server {
  #handler;
  mode = DEFAULT_STREAMING_MODE;

  constructor() {
    const conns = new Connections();
    this.#handler = new Handler(this, conns);
    this.start(conns);
  }

  async start(conns) {
    const app = express();
    app.use(express.json({ limit: "100mb" }));
    app.use(express.raw({ type: "*/*", limit: "100mb" }));

    app.get("/", (req, res) => res.status(conns.hasConn() ? 200 : 404).send(conns.hasConn() ? "✅ 代理就绪" : "❌ 无连接"));
    app.get("/favicon.ico", (req, res) => res.status(204).send());
    
    app.get("/admin/set-mode", (req, res) => {
      if (["fake", "real"].includes(req.query.mode)) {
        this.mode = req.query.mode;
        log("info", `模式切换: ${this.mode}`);
        return res.send(`模式已切换: ${this.mode}`);
      }
      res.status(400).send('无效模式');
    });
    app.get("/admin/get-mode", (req, res) => res.send(`当前模式: ${this.mode}`));
    
    app.get("/v1/models", (req, res) => this.#handler.handleModels(req, res));
    app.post("/v1/chat/completions", (req, res) => this.#handler.handle(req, res, true));
    app.all(/(.*)/, (req, res) => this.#handler.handle(req, res, false));

    const httpServer = http.createServer(app);
    const wss = new WebSocket.Server({ server: httpServer });
    wss.on("connection", (ws, req) => conns.add(ws, { address: req.socket.remoteAddress }));

    httpServer.listen(process.env.PORT || 7860, "0.0.0.0", () => {
      log("info", `服务启动于 http://0.0.0.0:${process.env.PORT || 7860}`);
      log("info", `模式: ${this.mode}`);
    });
  }
}

new Server();
