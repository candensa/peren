export default {
  fetch(request) {
    return Response.json({ path: new URL(request.url).pathname });
  },
};
