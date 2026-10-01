class EventSource extends event.EventTarget {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSED = 2;

  #url;
  #readyState = EventSource.CONNECTING;
  #closed = false;

  onopen = null;
  onmessage = null;
  onerror = null;
  withCredentials = false;

  constructor(url, init = {}) {
    super();
    this.#url = String(url);
    this.withCredentials = Boolean(init?.withCredentials);
    PromiseResolve().then(() => this.#connect());
  }

  get url() {
    return this.#url;
  }

  get readyState() {
    return this.#readyState;
  }

  close() {
    this.#closed = true;
    this.#readyState = EventSource.CLOSED;
  }

  async #connect() {
    try {
      const response = await outboundFetch(this.#url, {
        headers: { accept: "text/event-stream" },
      });
      if (this.#closed) return;
      if (response.status < 200 || response.status >= 300) {
        throw new TypeError(`EventSource request failed with status ${response.status}`);
      }
      this.#readyState = EventSource.OPEN;
      this.#emit("open", new event.Event("open"));
      const text = await response.text();
      if (this.#closed) return;
      this.#dispatchStream(text);
      if (!this.#closed) this.#readyState = EventSource.CLOSED;
    } catch (error) {
      if (this.#closed) return;
      this.#readyState = EventSource.CLOSED;
      const failure = new event.Event("error");
      failure.error = error;
      this.#emit("error", failure);
    }
  }

  #dispatchStream(text) {
    let name = "message";
    let data = [];
    let id = "";
    for (const line of String(text).replace(/\r\n/g, "\n").replace(/\r/g, "\n").split("\n")) {
      if (line === "") {
        if (data.length > 0) {
          this.#emit(name, new event.MessageEvent(name, {
            data: data.join("\n"),
            lastEventId: id,
            origin: new URL(this.#url).origin,
          }));
        }
        name = "message";
        data = [];
        continue;
      }
      if (line.startsWith(":")) continue;
      const colon = line.indexOf(":");
      const field = colon === -1 ? line : line.slice(0, colon);
      const value = colon === -1 ? "" : line.slice(colon + 1).replace(/^ /, "");
      if (field === "event") name = value || "message";
      if (field === "data") data.push(value);
      if (field === "id") id = value;
    }
  }

  #emit(type, dispatched) {
    this.dispatchEvent(dispatched);
    const handler = this[`on${type}`];
    if (typeof handler === "function") handler.call(this, dispatched);
  }
}

globalThis.EventSource = EventSource;
