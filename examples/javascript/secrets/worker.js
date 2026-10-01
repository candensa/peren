export default {
  fetch(_request, env) {
    return Response.json({
      message: env.MESSAGE,
      tokenLength: env.API_TOKEN.length,
    });
  },
};
