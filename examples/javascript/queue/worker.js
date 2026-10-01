export default {
  async fetch(request, env) {
    await env.JOBS.send({ path: new URL(request.url).pathname }, { dedupId: crypto.randomUUID() });
    return Response.json({ queued: true, provider: env.JOBS.provider.kind });
  },

  async queue(batch) {
    console.log("jobs ready after lease", batch.metrics.ready);
    batch.ackAll();
  },
};
