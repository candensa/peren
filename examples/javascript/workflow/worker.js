export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (request.method === "DELETE") {
      const id = url.searchParams.get("id");
      return Response.json(await env.FLOW.get(id).terminate("stopped"));
    }
    const instance = await env.FLOW.create();
    return Response.json(await instance.status());
  },
};
