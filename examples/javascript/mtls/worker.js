export default {
  async fetch(_request, env) {
    return fetch("https://example.com/", {
      cf: { mtlsCertificate: env.CLIENT_CERT },
    });
  },
};
