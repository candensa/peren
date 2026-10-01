export default {
  scheduled(event, _env, ctx) {
    ctx.waitUntil(Promise.resolve(event.cron));
  },

  fetch() {
    return new Response("cron service\n");
  },
};
