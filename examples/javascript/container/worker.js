export default {
  async fetch(request, env) {
    const upstream = await env.APP.fetch(request);
    if (upstream.ok) return upstream;
    return Response.json({
      image: env.APP.image,
      port: env.APP.port,
      upstream: upstream.status,
      detail: await upstream.text(),
    });
  },
};
