export default {
  async fetch(request) {
    return new Response(null, {
      status: 204,
      headers: { "x-peren-path": new URL(request.url).pathname },
    });
  },

  async session(name, details) {
    return { name, role: details.role };
  },
};
