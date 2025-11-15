const express = require("express");
const WebSocket = require("ws");
const http = require("http");
const { EventEmitter } = require("events");

const SECRET_KEY = process.env.MY_SECRET_KEY || "123456";
const DEFAULT_STREAMING_MODE = "real";

const log = (level, msg) =>
  console[level](`[${level.toUpperCase()}] ${new Date().toISOString()} - ${msg}`);

const genId = () => `${Date.now()}_${Math.random().toString(36).slice(2, 11)}`;

class Queue extends EventEmitter {
  constructor() {
    super();
    this.msgs = [];
    this.waiters = [];
    this.closed = false;
  }

  push(msg) {
    if (this.closed) return;
    const waiter = this.waiters.shift();
    waiter ? waiter.resolve(msg) : this.msgs.push(msg);
  }

  async pop() {
    if (this.closed) throw new Error("Queue closed");
    if (this.msgs.length) return this.msgs.shift();
    return new Promise((resolve, reject) => {
      this.waiters.push({ resolve, reject });
    });
  }

  close() {
    this.closed = true;
    this.waiters.forEach((w) => w.reject(new Error("Queue closed")));
    this.waiters = [];
    this.msgs = [];
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

class Handler {
  constructor(server, conns) {
    this.server = server;
    this.conns = conns;
  }

  async handle(req, res) {
    if (req.path === "/v1/models") {
      return this._handleModels(req, res);
    }

    if (!this._auth(req, res)) return;
    if (!this._checkConn(res)) return;

    const id = genId();
    const isOpenAI = req.path.includes("/v1/chat/completions");
    
    try {
      const proxyReq = this._buildReq(req, id, isOpenAI);
      const queue = this.conns.createQueue(id);

      try {
        await this._dispatch(req, res, proxyReq, queue, isOpenAI);
      } catch (err) {
        this._error(err, res, isOpenAI);
      } finally {
        this.conns.removeQueue(id);
      }
    } catch (err) {
      log("error", `构建请求失败: ${err.message}`);
      this._error(err, res, isOpenAI);
    }
  }

  async _handleModels(req, res) {
    log("info", "模型列表请求");
    
    if (!this.conns.hasConn()) {
      return res.status(503).json({
        error: {
          message: "无可用连接",
          type: "service_unavailable",
          code: "no_connection",
        },
      });
    }

    const id = genId();
    const proxyReq = {
      path: "/v1beta/models",
      method: "GET",
      headers: {},
      query_params: {},
      body: "",
      request_id: id,
      streaming_mode: "real",
      is_openai: false,
    };

    const queue = this.conns.createQueue(id);

    try {
      this._forward(proxyReq);
      const header = await queue.pop();
      
      if (header.event_type === "error") {
        return res.status(header.status || 500).json({
          error: {
            message: header.message,
            type: "api_error",
            code: "model_list_error",
          },
        });
      }

      const data = await queue.pop();
      await queue.pop();

      if (data.data) {
        const geminiModels = JSON.parse(data.data);
        const openaiModels = this._convertModelsToOpenAI(geminiModels);
        res.status(200).json(openaiModels);
      } else {
        res.status(500).json({
          error: {
            message: "无法获取模型列表",
            type: "api_error",
            code: "empty_response",
          },
        });
      }
    } catch (err) {
      log("error", `获取模型列表失败: ${err.message}`);
      res.status(500).json({
        error: {
          message: `获取模型列表失败: ${err.message}`,
          type: "proxy_error",
          code: "model_list_error",
        },
      });
    } finally {
      this.conns.removeQueue(id);
    }
  }

  _convertModelsToOpenAI(geminiData) {
    const models = geminiData.models || [];
    const openaiModels = models
      .filter(model => {
        return model.supportedGenerationMethods?.includes("generateContent");
      })
      .map(model => ({
        id: model.name.replace("models/", ""),
        object: "model",
        created: Math.floor(new Date(model.updateTime || Date.now()).getTime() / 1000),
        owned_by: "google",
        permission: [],
        root: model.name.replace("models/", ""),
        parent: null,
      }));

    return {
      object: "list",
      data: openaiModels,
    };
  }

  _auth(req, res) {
    let key = req.query.key;
    
    if (!key && req.headers.authorization) {
      const auth = req.headers.authorization;
      if (auth.startsWith("Bearer ")) {
        key = auth.substring(7);
      }
    }

    if (key === SECRET_KEY) {
      delete req.query.key;
      log("info", `验证通过: ${req.method} ${req.path}`);
      return true;
    }
    
    log("warn", `验证失败: ${req.path}`);
    this._send(res, 401, "Unauthorized", false);
    return false;
  }

  _checkConn(res) {
    if (this.conns.hasConn()) return true;
    this._send(res, 503, "无可用连接", false);
    return false;
  }

  _buildReq(req, id, isOpenAI) {
    let body = "";
    let convertedBody = req.body;

    if (isOpenAI && req.body) {
      log("info", `原始 OpenAI 请求: ${JSON.stringify(req.body, null, 2)}`);
      try {
        convertedBody = this._convertOpenAIToGemini(req.body);
      } catch (err) {
        log("error", `转换失败: ${err.message}`);
        throw err;
      }
    }

    if (Buffer.isBuffer(convertedBody)) body = convertedBody.toString("utf-8");
    else if (typeof convertedBody === "string") body = convertedBody;
    else if (convertedBody) body = JSON.stringify(convertedBody);

    const finalReq = {
      path: isOpenAI ? this._convertOpenAIPath(req) : req.path,
      method: req.method,
      headers: req.headers,
      query_params: isOpenAI ? this._convertOpenAIQuery(req) : req.query,
      body,
      request_id: id,
      streaming_mode: this.server.mode,
      is_openai: isOpenAI,
    };

    log("info", `最终请求路径: ${finalReq.path}`);
    log("info", `查询参数: ${JSON.stringify(finalReq.query_params)}`);

    return finalReq;
  }

  _convertOpenAIPath(req) {
    const model = req.body?.model || "gemini-2.0-flash-exp";
    const isStream = req.body?.stream === true;
    const action = isStream ? "streamGenerateContent" : "generateContent";
    return `/v1beta/models/${model}:${action}`;
  }

  _convertOpenAIQuery(req) {
    const query = { ...req.query };
    delete query.key;
    
    if (req.body?.stream === true) {
      query.alt = "sse";
    }
    return query;
  }

  _convertOpenAIToGemini(openaiBody) {
    const messages = openaiBody.messages || [];
    const contents = [];
    const systemMessages = [];

    for (const msg of messages) {
      if (msg.role === "system") {
        systemMessages.push(msg.content);
      } else {
        contents.push({
          role: msg.role === "assistant" ? "model" : "user",
          parts: [{ text: msg.content }],
        });
      }
    }

    if (contents.length === 0) {
      log("error", "没有有效的用户消息");
      throw new Error("至少需要一条用户或助手消息");
    }

    const geminiBody = { contents };
    
    if (systemMessages.length > 0) {
      geminiBody.systemInstruction = {
        parts: [{ text: systemMessages.join("\n\n") }]
      };
    }

    const genConfig = {};
    
    if (openaiBody.temperature !== undefined) {
      genConfig.temperature = Math.max(0, Math.min(2, openaiBody.temperature));
    }
    if (openaiBody.max_tokens !== undefined) {
      genConfig.maxOutputTokens = openaiBody.max_tokens;
    }
    if (openaiBody.top_p !== undefined) {
      genConfig.topP = Math.max(0, Math.min(1, openaiBody.top_p));
    }
    if (openaiBody.top_k !== undefined) {
      genConfig.topK = Math.max(1, Math.floor(openaiBody.top_k));
    }
    
    if (openaiBody.presence_penalty !== undefined) {
      const penalty = Math.max(-2, Math.min(2, openaiBody.presence_penalty));
      if (penalty !== 0) {
        genConfig.presencePenalty = penalty;
      }
    }
    if (openaiBody.frequency_penalty !== undefined) {
      const penalty = Math.max(-2, Math.min(2, openaiBody.frequency_penalty));
      if (penalty !== 0) {
        genConfig.frequencyPenalty = penalty;
      }
    }
    if (openaiBody.stop !== undefined) {
      genConfig.stopSequences = Array.isArray(openaiBody.stop) 
        ? openaiBody.stop 
        : [openaiBody.stop];
    }
    
    // 思考配置 - 只支持数值形式的 token 预算
    if (openaiBody.thinking_budget !== undefined && openaiBody.thinking_budget > 0) {
      genConfig.thinkingConfig = {
        thoughtGenerationTokenBudget: Math.max(1, Math.floor(openaiBody.thinking_budget))
      };
      log("info", `思考预算: ${genConfig.thinkingConfig.thoughtGenerationTokenBudget} tokens`);
    }

    if (Object.keys(genConfig).length > 0) {
      geminiBody.generationConfig = genConfig;
    }

    log("info", `转换后的请求体: ${JSON.stringify(geminiBody, null, 2)}`);

    return geminiBody;
  }

  async _dispatch(req, res, proxyReq, queue, isOpenAI) {
    const mode = this.server.mode;
    const stream = isOpenAI 
      ? req.body?.stream === true 
      : req.path.includes("streamGenerateContent");

    if (mode === "fake") {
      return stream
        ? this._fakeStream(req, res, proxyReq, queue, isOpenAI)
        : this._fakeNonStream(res, proxyReq, queue, isOpenAI);
    }
    return this._realStream(res, proxyReq, queue, stream, isOpenAI);
  }

  async _fakeNonStream(res, proxyReq, queue, isOpenAI) {
    log("info", "非流式请求");
    this._forward(proxyReq);

    const header = await queue.pop();
    if (header.event_type === "error") {
      return this._send(res, header.status || 500, header.message, isOpenAI);
    }

    this._setHeaders(res, header);
    const data = await queue.pop();
    await queue.pop();

    if (data.data) {
      if (isOpenAI) {
        log("info", `Gemini 原始响应: ${data.data.substring(0, 500)}...`);
        const responseData = this._convertGeminiToOpenAI(data.data, false);
        res.json(JSON.parse(responseData));
      } else {
        res.send(data.data);
      }
    }
    log("info", "JSON响应完成");
  }

  async _fakeStream(req, res, proxyReq, queue, isOpenAI) {
    this._sseHeaders(res);
    const keepAlive = isOpenAI 
      ? this._openAIKeepAlive() 
      : this._keepAlive(req.path);
    const timer = setInterval(() => res.write(keepAlive), 1000);

    try {
      log("info", "假流式开始");
      this._forward(proxyReq);

      const header = await queue.pop();
      if (header.event_type === "error") {
        this._sseError(res, header.message, isOpenAI);
        throw new Error(header.message);
      }

      const data = await queue.pop();
      await queue.pop();

      if (data.data) {
        if (isOpenAI) {
          log("info", `Gemini 原始响应: ${data.data.substring(0, 500)}...`);
          const responseData = this._convertGeminiToOpenAI(data.data, true);
          res.write(responseData);
          res.write("data: [DONE]\n\n");
        } else {
          res.write(`data: ${data.data}\n\n`);
        }
        log("info", "SSE响应完成");
      }
    } finally {
      clearInterval(timer);
      if (!res.writableEnded) res.end();
      log("info", "假流式结束");
    }
  }

  async _realStream(res, proxyReq, queue, isStreamReq, isOpenAI) {
    this._forward(proxyReq);
    const header = await queue.pop();

    if (header.event_type === "error") {
      return this._send(res, header.status, header.message, isOpenAI);
    }

    if (isStreamReq && !header.headers?.["content-type"]) {
      header.headers = header.headers || {};
      header.headers["content-type"] = "text/event-stream";
    }

    this._setHeaders(res, header);
    log("info", "真流式开始");

    let fullResponse = "";
    let chunkCount = 0;

    try {
      while (true) {
        const data = await queue.pop();
        if (data.type === "STREAM_END") break;
        
        if (data.data) {
          chunkCount++;
          if (chunkCount <= 2) {
            log("info", `收到数据块 #${chunkCount}: ${data.data.substring(0, 200)}...`);
          }
          
          if (isOpenAI && isStreamReq) {
            const converted = this._convertGeminiSSEToOpenAI(data.data);
            res.write(converted);
          } else if (isOpenAI && !isStreamReq) {
            fullResponse += data.data;
          } else {
            res.write(data.data);
          }
        }
      }
      
      if (isOpenAI && isStreamReq) {
        res.write("data: [DONE]\n\n");
      } else if (isOpenAI && !isStreamReq) {
        log("info", `完整响应: ${fullResponse.substring(0, 500)}...`);
        const jsonResponse = this._convertGeminiToOpenAI(fullResponse, false);
        res.json(JSON.parse(jsonResponse));
      }
    } finally {
      if (!res.writableEnded) res.end();
      log("info", `真流式结束 (共 ${chunkCount} 个数据块)`);
    }
  }

  _convertGeminiToOpenAI(geminiData, isStream) {
    try {
      const gemini = typeof geminiData === "string" ? JSON.parse(geminiData) : geminiData;
      const candidate = gemini.candidates?.[0];
      
      let text = "";
      let thinkingText = null;
      
      if (candidate?.content?.parts) {
        for (const part of candidate.content.parts) {
          if (part.thought === true) {
            thinkingText = (thinkingText || "") + (part.text || "");
          } else if (part.text) {
            text += part.text;
          }
        }
      }
      
      const finishReason = candidate?.finishReason;
      const usage = gemini.usageMetadata;

      log("info", `提取内容 - 文本长度: ${text.length}, 思考长度: ${thinkingText ? thinkingText.length : 0}`);
      if (usage) {
        log("info", `Token 使用: prompt=${usage.promptTokenCount}, completion=${usage.candidatesTokenCount}, total=${usage.totalTokenCount}`);
      }

      if (isStream) {
        const delta = {};
        if (text) delta.content = text;
        if (thinkingText) delta.reasoning_content = thinkingText;

        const chunk = {
          id: `chatcmpl-${genId()}`,
          object: "chat.completion.chunk",
          created: Math.floor(Date.now() / 1000),
          model: "gpt-4",
          choices: [{
            index: 0,
            delta: delta,
            finish_reason: finishReason === "STOP" ? "stop" : null,
          }],
        };
        
        if (finishReason === "STOP" && usage) {
          chunk.usage = {
            prompt_tokens: usage.promptTokenCount || 0,
            completion_tokens: usage.candidatesTokenCount || 0,
            total_tokens: usage.totalTokenCount || 0,
          };
        }
        
        return `data: ${JSON.stringify(chunk)}\n\n`;
      } else {
        const message = { role: "assistant", content: text };
        if (thinkingText) {
          message.reasoning_content = thinkingText;
        }

        const response = {
          id: `chatcmpl-${genId()}`,
          object: "chat.completion",
          created: Math.floor(Date.now() / 1000),
          model: "gpt-4",
          choices: [{
            index: 0,
            message: message,
            finish_reason: finishReason === "STOP" ? "stop" : "length",
          }],
          usage: {
            prompt_tokens: usage?.promptTokenCount || 0,
            completion_tokens: usage?.candidatesTokenCount || 0,
            total_tokens: usage?.totalTokenCount || 0,
          },
        };

        return JSON.stringify(response);
      }
    } catch (err) {
      log("error", `转换失败: ${err.message}`);
      log("error", `原始数据: ${JSON.stringify(geminiData).substring(0, 500)}`);
      return geminiData;
    }
  }

  _convertGeminiSSEToOpenAI(sseData) {
    const lines = sseData.split("\n");
    let result = "";

    for (const line of lines) {
      if (!line.startsWith("data: ")) continue;
      
      try {
        const gemini = JSON.parse(line.slice(6));
        const candidate = gemini.candidates?.[0];
        
        let text = "";
        let thinkingText = null;
        
        if (candidate?.content?.parts) {
          for (const part of candidate.content.parts) {
            if (part.thought === true) {
              thinkingText = (thinkingText || "") + (part.text || "");
            } else if (part.text) {
              text += part.text;
            }
          }
        }
        
        const finishReason = candidate?.finishReason;
        const usage = gemini.usageMetadata;

        const delta = {};
        if (text) delta.content = text;
        if (thinkingText) delta.reasoning_content = thinkingText;

        const chunk = {
          id: `chatcmpl-${genId()}`,
          object: "chat.completion.chunk",
          created: Math.floor(Date.now() / 1000),
          model: "gpt-4",
          choices: [{
            index: 0,
            delta: delta,
            finish_reason: finishReason === "STOP" ? "stop" : null,
          }],
        };
        
        if (finishReason === "STOP" && usage) {
          chunk.usage = {
            prompt_tokens: usage.promptTokenCount || 0,
            completion_tokens: usage.candidatesTokenCount || 0,
            total_tokens: usage.totalTokenCount || 0,
          };
        }
        
        result += `data: ${JSON.stringify(chunk)}\n\n`;
      } catch (err) {
        result += line + "\n";
      }
    }

    return result;
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

  _openAIKeepAlive() {
    return `data: ${JSON.stringify({
      id: `chatcmpl-${genId()}`,
      object: "chat.completion.chunk",
      created: Math.floor(Date.now() / 1000),
      model: "gpt-4",
      choices: [{ index: 0, delta: {}, finish_reason: null }],
    })}\n\n`;
  }

  _sseError(res, msg, isOpenAI) {
    if (res.writableEnded) return;
    
    res.write(
      `data: ${JSON.stringify({
        error: {
          message: `[代理] ${msg}`,
          type: "proxy_error",
          code: "proxy_error",
        },
      })}\n\n`
    );
  }

  _error(err, res, isOpenAI) {
    if (res.headersSent) {
      log("error", `错误(头已发): ${err.message}`);
      if (this.server.mode === "fake") this._sseError(res, err.message, isOpenAI);
      if (!res.writableEnded) res.end();
      return;
    }
    log("error", `错误: ${err.message}`);
    this._send(res, 500, `代理错误: ${err.message}`, isOpenAI);
  }

  _send(res, status, msg, isOpenAI) {
    if (res.headersSent) return;
    
    if (isOpenAI && status >= 400) {
      res.status(status).json({
        error: {
          message: msg,
          type: "proxy_error",
          code: "proxy_error",
        },
      });
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
    app.use(express.urlencoded({ extended: true, limit: "100mb" }));
    app.use(express.raw({ type: "*/*", limit: "100mb" }));

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
    const wss = new WebSocket.Server({ server: httpServer });

    wss.on("connection", (ws, req) => {
      this.conns.add(ws, { address: req.socket.remoteAddress });
    });

    return new Promise((resolve) => {
      httpServer.listen(process.env.PORT || 7860, "0.0.0.0", () => {
        log("info", `HTTP: http://0.0.0.0:${process.env.PORT || 7860}`);
        log("info", `WS: ws://0.0.0.0:${process.env.PORT || 7860}`);
        log("info", `模式: ${this.mode}`);
        resolve();
      });
    });
  }
}

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
