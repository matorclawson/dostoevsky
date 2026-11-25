export class ClientError extends Error {
  constructor(status, code) {
    super(`${status} ${code}`);
    this.status = status;
    this.code = code;
  }
}

function receipt(headers) {
  return {
    partition: Number(headers.get("Dostoevsky-Partition")),
    offset: BigInt(headers.get("Dostoevsky-Offset")),
  };
}

export class Client {
  #base;
  #token;
  constructor(url, token) {
    this.#base = url.replace(/\/+$/, "") + "/dostoevsky/client/v1/";
    this.#token = token;
  }
  async #call(op, args, body) {
    const url = new URL(this.#base + op);
    for (const [k, v] of Object.entries(args)) url.searchParams.set(k, String(v));
    const res = await fetch(url, {
      method: body === undefined ? "GET" : "POST", body, redirect: "error",
      headers: { Authorization: "Bearer " + this.#token, "Content-Type": "application/octet-stream" },
      signal: AbortSignal.timeout(40000),
    });
    if (!res.ok) throw new ClientError(res.status, (await res.text()).trim());
    return res;
  }
  async publish(topic, value, { key = "", id = "" } = {}) {
    const res = await this.#call("publish", { topic, key, id }, value);
    await res.arrayBuffer();
    return receipt(res.headers);
  }
  async read(topic, partition, offset, waitMs = 0) {
    const res = await this.#call("read", { topic, partition, offset, wait_ms: waitMs });
    if (res.status === 204) return null;
    return { ...receipt(res.headers), key: decodeURIComponent(res.headers.get("Dostoevsky-Key")),
      id: decodeURIComponent(res.headers.get("Dostoevsky-ID")), time: res.headers.get("Dostoevsky-Time"),
      value: new Uint8Array(await res.arrayBuffer()) };
  }
  async position(topic, partition, group) {
    const res = await this.#call("position", { topic, partition, group });
    return { next: BigInt(res.headers.get("Dostoevsky-Next")), state: new Uint8Array(await res.arrayBuffer()) };
  }
  async commit(topic, partition, group, expected, state = new Uint8Array()) {
    await this.#call("commit", { topic, partition, group, expected }, state);
  }
}
