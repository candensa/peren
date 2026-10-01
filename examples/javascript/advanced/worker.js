export default {
  async fetch(request, env, ctx) {
    const gate = await env.LIMIT.limit({ key: "local" });
    if (!gate.success) {
      return Response.json({ ok: false, remaining: gate.remaining }, { status: 429 });
    }
    ctx.waitUntil(env.JOBS.send({ url: request.url }));
    await env.CACHE.put("last", request.url);
    await env.DB.exec("create table if not exists hits (id integer primary key)");
    await env.DB.prepare("insert into hits default values").run();
    const row = await env.DB.prepare("select count(*) as count from hits").first();
    return Response.json({
      ok: true,
      last: await env.CACHE.get("last"),
      remaining: gate.remaining,
      hits: row.count,
    });
  },
};
