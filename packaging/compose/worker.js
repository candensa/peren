export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/health") {
      return Response.json({ ok: true, message: env.MESSAGE });
    }
    return new Response(env.MESSAGE + "\n", {
      headers: { "content-type": "text/plain; charset=utf-8" },
    });
  },
};
