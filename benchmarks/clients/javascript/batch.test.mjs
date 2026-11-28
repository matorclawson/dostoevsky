import test from "node:test";
import assert from "node:assert/strict";
import { runWorkload, verify, STOP } from "./core.mjs";

test("one broker batch per chunk including the partial tail", async () => {
  const stored = [];
  const sizes = [];
  const adapter = {
    publishBatch: async values => { sizes.push(values.length); return values.map(v => stored.push(v) - 1); },
    read: async n => stored[Number(n)],
  };
  const result = await runWorkload(adapter, 1, 7, 0, undefined, 3);
  assert.deepEqual(sizes, [3,3,1]);
  assert.equal(result.hist.total(), 3);
  assert.equal(await verify(adapter, result.records), "");
});

test("verify individual acknowledgements even when a pipelined peer fails", async () => {
  const stored = [];
  const adapter = {
    publish: async value => {
      if (value.readBigUInt64BE() === 1n) throw new Error("failed");
      return stored.push(value) - 1;
    },
    read: async n => stored[Number(n)],
  };
  const result = await runWorkload(adapter, 1, 9, 0, undefined, 3);
  assert.equal(result.stop, STOP.failed);
  assert.equal(result.acked, 2);
  assert.equal(await verify(adapter, result.records), "");
});
