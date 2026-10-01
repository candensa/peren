export default {
  async fetch(request) {
    const cache = await caches.open("pages");
    const cached = await cache.match(request);
    if (cached) return cached;

    const response = Response.json({ source: "origin", provider: cache.provider.kind });
    await cache.put(request, response.clone());
    return response;
  },
};
