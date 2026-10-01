export default {
  async fetch(_request, env) {
    const response = await env.PUBLIC_API.fetch("https://example.com/");
    return Response.json({ status: response.status, allowedHosts: env.PUBLIC_API.allowedHosts });
  },
};
