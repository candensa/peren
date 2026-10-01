export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const key = url.searchParams.get("key") ?? "message";
    if (request.method === "PUT") {
      await env.KV.compareAndSet(key, { missing: true }, await request.text());
      return new Response(null, { status: 204 });
    }
    const value = await env.KV.get(key);
    const record = await env.KV.getWithMetadata(key);
    return Response.json({ provider: env.KV.provider.kind, value: value ?? "", version: record.version });
  },
};
