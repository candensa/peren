export default {
  async fetch(request, env) {
    const key = new URL(request.url).searchParams.get("key") ?? "local";
    const result = await env.LIMIT.limit({ key });
    return Response.json(result, { status: result.success ? 200 : 429 });
  },
};
