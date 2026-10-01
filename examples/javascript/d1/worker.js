export default {
  async fetch(_request, env) {
    await env.DB.exec("create table if not exists visits (id integer primary key)");
    await env.DB.prepare("insert into visits default values").run();
    const row = await env.DB.prepare("select count(*) as count from visits").first();
    return Response.json({ provider: env.DB.provider.kind, ...row });
  },
};
