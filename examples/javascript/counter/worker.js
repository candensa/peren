export class Counter {
  constructor(state) {
    this.state = state;
  }

  async fetch() {
    const value = ((await this.state.storage.get("value")) ?? 0) + 1;
    await this.state.storage.put("value", value);
    return Response.json({ value });
  }
}

export default {
  fetch(request, env) {
    const id = env.COUNTER.idFromName("main");
    return env.COUNTER.get(id).fetch(request);
  },
};
