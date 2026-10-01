export default {
  fetch(_request, env) {
    env.ANALYTICS.writeDataPoint({
      blobs: ["request"],
      doubles: [1],
      indexes: ["demo"],
    });
    return Response.json({
      dataset: env.ANALYTICS.provider.dataset,
      kind: env.ANALYTICS.provider.kind,
    });
  },
};
