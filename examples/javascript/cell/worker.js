export default {
  async fetch() {
    let visits = 0;
    await Peren.storage.transaction(async (storage) => {
      const current = await storage.get("visits");
      visits = Number(new TextDecoder().decode(current ?? new Uint8Array())) + 1;
      await storage.put("visits", new TextEncoder().encode(String(visits)));
    });
    return Response.json({ visits });
  },
};
