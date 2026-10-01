export default {
  fetch(_request, env) {
    return Response.json({
      provider: env.DB.provider.kind,
      connectionString: env.DB.connectionString,
      poolMaxConnections: env.DB.poolMaxConnections,
      cachingDisabled: env.DB.cachingDisabled,
    });
  },
};
