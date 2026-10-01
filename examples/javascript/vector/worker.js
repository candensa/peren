export default {
  async fetch(_request, env) {
    await env.VECTORS.upsert([
      { id: "intro", values: [1, 0, 0], metadata: { title: "Intro" } },
      { id: "ops", values: [0, 1, 0], metadata: { title: "Operations" } },
    ]);

    const result = await env.VECTORS.query([1, 0, 0], {
      topK: 1,
      filter: { title: "Intro" },
      returnMetadata: true,
    });

    return Response.json({ provider: env.VECTORS.provider.kind, matches: result.matches });
  },
};
