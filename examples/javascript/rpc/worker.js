export default {
  async fetch(_request, env) {
    const session = await env.AUTH.session("ada", { role: "admin" });
    const probe = await env.AUTH.fetch("https://auth.internal/health");
    return Response.json({ session, probe: probe.status });
  },
};
