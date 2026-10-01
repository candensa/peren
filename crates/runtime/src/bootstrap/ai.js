class Ai {}
class AnalyticsEngineDataset {}
class Container {}
class Hyperdrive {}
class ImagesBinding {}
class Loader {}
class RateLimiter {}
class VectorizeIndex {}


function aiText(prompt) {
  if (typeof prompt === "string") return prompt;
  if (prompt?.prompt !== undefined) return String(prompt.prompt);
  if (prompt?.text !== undefined) return String(prompt.text);
  if (prompt?.input !== undefined) return String(prompt.input);
  return String(prompt ?? "");
}

function aiMessages(messages) {
  return ArrayFrom(messages ?? [], (message) => ({ ...message }));
}

function aiOptions(options = {}) {
  const body = { ...options };
  delete body.headers;
  delete body.maxTokens;
  delete body.maxOutputTokens;
  return body;
}

function aiLimit(options = {}, fallback = undefined) {
  const value = options.max_tokens ?? options.maxTokens ?? options.maxOutputTokens ?? options.max_output_tokens ?? fallback;
  return value === undefined ? undefined : Number(value);
}

function aiTextInput(route, model, prompt, options = {}) {
  const text = aiText(prompt);
  const body = aiOptions(options);
  if (route.kind === "open_ai") {
    const limit = aiLimit(options);
    if (limit !== undefined) body.max_output_tokens = limit;
    return Object.assign({ input: text }, body);
  }
  if (route.kind === "anthropic") {
    return Object.assign({
      messages: [{ role: "user", content: text }],
      max_tokens: aiLimit(options, 1024),
    }, body);
  }
  if (route.kind === "gemini") {
    const generationConfig = {};
    const limit = aiLimit(options);
    if (limit !== undefined) generationConfig.maxOutputTokens = limit;
    if (options.temperature !== undefined) generationConfig.temperature = Number(options.temperature);
    delete body.temperature;
    const request = Object.assign({ contents: [{ role: "user", parts: [{ text }] }] }, body);
    if (Object.keys(generationConfig).length > 0) request.generationConfig = generationConfig;
    return request;
  }
  return Object.assign({ prompt: text }, body);
}

function aiChatInput(route, messages, options = {}) {
  const list = aiMessages(messages);
  const body = aiOptions(options);
  if (route.kind === "open_ai") {
    const limit = aiLimit(options);
    if (limit !== undefined) body.max_output_tokens = limit;
    return Object.assign({ input: list }, body);
  }
  if (route.kind === "anthropic") {
    return Object.assign({
      messages: list,
      max_tokens: aiLimit(options, 1024),
    }, body);
  }
  if (route.kind === "gemini") {
    const generationConfig = {};
    const limit = aiLimit(options);
    if (limit !== undefined) generationConfig.maxOutputTokens = limit;
    if (options.temperature !== undefined) generationConfig.temperature = Number(options.temperature);
    delete body.temperature;
    const request = Object.assign({
      contents: list.map((message) => ({
        role: message.role === "assistant" ? "model" : String(message.role ?? "user"),
        parts: Array.isArray(message.parts) ? message.parts : [{ text: String(message.content ?? message.text ?? "") }],
      })),
    }, body);
    if (Object.keys(generationConfig).length > 0) request.generationConfig = generationConfig;
    return request;
  }
  return Object.assign({ messages: list }, body);
}

function aiEmbedInput(route, input, options = {}) {
  const text = typeof input === "string" ? input : input?.text ?? input?.input ?? input;
  const body = aiOptions(options);
  if (route.kind === "open_ai") return Object.assign({ input: text }, body);
  if (route.kind === "gemini") return Object.assign({ content: { parts: [{ text: String(text ?? "") }] } }, body);
  return Object.assign({ text }, body);
}

function aiTextOutput(raw) {
  if (typeof raw === "string") return { text: raw, raw };
  if (raw?.output_text !== undefined) return { text: String(raw.output_text), raw };
  if (Array.isArray(raw?.output)) {
    const text = raw.output
      .flatMap((item) => Array.isArray(item?.content) ? item.content : [])
      .filter((item) => item?.type === "output_text" || item?.text !== undefined)
      .map((item) => String(item.text ?? ""))
      .join("");
    if (text.length > 0) return { text, raw };
  }
  if (raw?.content !== undefined) {
    if (typeof raw.content === "string") return { text: raw.content, raw };
    if (Array.isArray(raw.content)) {
      const text = raw.content
        .filter((item) => item?.type === "text" || item?.text !== undefined)
        .map((item) => String(item.text ?? ""))
        .join("");
      if (text.length > 0) return { text, raw };
    }
  }
  const gemini = raw?.candidates?.[0]?.content?.parts;
  if (Array.isArray(gemini)) {
    const text = gemini.map((part) => String(part.text ?? "")).join("");
    if (text.length > 0) return { text, raw };
  }
  if (raw?.response !== undefined) return { text: String(raw.response), raw };
  if (raw?.result?.response !== undefined) return { text: String(raw.result.response), raw };
  if (raw?.result?.text !== undefined) return { text: String(raw.result.text), raw };
  if (raw?.text !== undefined) return { text: String(raw.text), raw };
  return { text: "", raw };
}

function aiEmbeddingOutput(raw) {
  if (Array.isArray(raw)) return { embeddings: raw, raw };
  if (Array.isArray(raw?.data)) {
    return {
      embeddings: raw.data.map((item) => item.embedding ?? item.values ?? item.vector ?? item),
      raw,
    };
  }
  if (Array.isArray(raw?.embedding)) return { embeddings: [raw.embedding], raw };
  if (Array.isArray(raw?.embeddings)) return { embeddings: raw.embeddings, raw };
  if (Array.isArray(raw?.result?.data)) {
    return {
      embeddings: raw.result.data.map((item) => item.embedding ?? item.values ?? item.vector ?? item),
      raw,
    };
  }
  if (Array.isArray(raw?.result?.embedding)) return { embeddings: [raw.result.embedding], raw };
  return { embeddings: [], raw };
}

async function aiProviderFetch(route, endpoint, target, body, options = {}) {
  if (endpoint.length === 0) throw new TypeError("AI endpoint is empty");
  const headers = Object.assign({ "content-type": "application/json" }, options.headers ?? {});
  if (route.authorization) headers.authorization = route.authorization;
  if (route.xApiKey) headers["x-api-key"] = route.xApiKey;
  if (route.anthropicVersion) headers["anthropic-version"] = route.anthropicVersion;
  const response = await outboundFetch(target, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });
  const contentType = response.headers.get("content-type") ?? "";
  if (contentType.includes("application/json")) return await response.json();
  return await response.text();
}

function aiBinding(binding) {
  const route = binding.route ?? binding.provider ?? { kind: "http", url: binding.endpoint };
  const endpoint = String(route.url ?? binding.endpoint ?? "").replace(/\/$/, "");
  const publicEndpoint = route.kind === "workers_ai"
    ? endpoint.replace(/\/accounts\/[^/]+\/ai\/run$/, "/accounts/redacted/ai/run")
    : endpoint;
  const provider = ObjectFreeze({ kind: route.kind ?? "http", endpoint: publicEndpoint });
  return ObjectFreeze(Object.assign(ObjectCreate(Ai.prototype), {
    endpoint: publicEndpoint,
    provider,
    async run(model, input = {}, options = {}) {
      if (route.kind === "local") {
        return await core.ops.op_ai_run({
          command: String(route.command ?? ""),
          model: String(model),
          input,
          options,
        });
      }
      if (endpoint.length === 0) throw new TypeError("AI endpoint is empty");
      let target = `${endpoint}/run/${encodeURIComponent(String(model))}`;
      let body = input;
      if (route.kind === "open_ai") {
        target = `${endpoint}/responses`;
        body = Object.assign({ model: String(model) }, input);
      } else if (route.kind === "anthropic") {
        target = `${endpoint}/messages`;
        body = Object.assign({ model: String(model) }, input);
      } else if (route.kind === "gemini") {
        target = `${endpoint}/models/${encodeURIComponent(String(model))}:generateContent?key=${encodeURIComponent(String(route.apiKey ?? ""))}`;
      } else if (route.kind === "workers_ai") {
        target = `${endpoint}/${encodeURIComponent(String(model))}`;
      }
      return await aiProviderFetch(route, endpoint, target, body, options);
    },
    async embed(model, input, options = {}) {
      if (route.kind === "local") {
        const text = typeof input === "string" ? input : input?.text ?? input;
        return await this.run(model, { text }, options);
      }
      const body = aiEmbedInput(route, input, options);
      if (route.kind === "open_ai") {
        return aiEmbeddingOutput(await aiProviderFetch(route, endpoint, `${endpoint}/embeddings`, Object.assign({ model: String(model) }, body), options));
      }
      if (route.kind === "gemini") {
        const target = `${endpoint}/models/${encodeURIComponent(String(model))}:embedContent?key=${encodeURIComponent(String(route.apiKey ?? ""))}`;
        return aiEmbeddingOutput(await aiProviderFetch(route, endpoint, target, body, options));
      }
      return aiEmbeddingOutput(await this.run(model, body, options));
    },
    async generateText(model, prompt, options = {}) {
      if (route.kind === "local") {
        const input = typeof prompt === "string" ? { prompt } : prompt ?? {};
        return await this.run(model, input, options);
      }
      return aiTextOutput(await this.run(model, aiTextInput(route, model, prompt, options)));
    },
    async chat(model, messages = [], options = {}) {
      if (route.kind === "local") return await this.run(model, { messages: ArrayFrom(messages) }, options);
      return aiTextOutput(await this.run(model, aiChatInput(route, messages, options)));
    },
  }));
}
