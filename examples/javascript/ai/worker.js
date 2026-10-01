const MODEL = {
  open_ai: "gpt-4.1-mini",
  anthropic: "claude-3-5-sonnet-latest",
  gemini: "gemini-1.5-flash",
  workers_ai: "@cf/meta/llama-3.1-8b-instruct",
  local: "local-summary",
};

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const prompt = url.searchParams.get("prompt") ?? "Describe Peren in one sentence.";
    const provider = env.AI.provider.kind;
    const model = url.searchParams.get("model") ?? MODEL[provider] ?? "default";
    const result = await env.AI.generateText(model, prompt, { maxTokens: 256 });

    return Response.json({
      provider,
      endpoint: env.AI.provider.endpoint,
      model,
      text: result.text,
      raw: result.raw,
    });
  },
};
