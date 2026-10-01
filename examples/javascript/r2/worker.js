export default {
  async fetch(request, env) {
    const key = new URL(request.url).pathname.slice(1) || "message";
    if (request.method === "PUT") {
      await env.BUCKET.put(key, request.body, { customMetadata: { uploadedBy: "example" } });
      return new Response(null, { status: 204 });
    }
    const object = await env.BUCKET.get(key);
    if (object === null) return Response.json({ provider: env.BUCKET.provider.kind, found: false }, { status: 404 });
    return new Response(object.body, {
      headers: {
        "x-peren-r2-provider": env.BUCKET.provider.kind,
        "x-peren-r2-owner": object.customMetadata.uploadedBy ?? "",
      },
    });
  },
};
