export default {
  async fetch(_request, env) {
    const response = await env.IMAGES.input("peren").transform({ width: 64 }).output();
    return new Response(await response.arrayBuffer(), {
      headers: { "content-type": response.headers.get("content-type") ?? "application/octet-stream" },
    });
  },
};
