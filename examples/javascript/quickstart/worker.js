export default {
  fetch(_request, env) {
    return Response.json({ product: "peren", status: env.MESSAGE });
  },
};
