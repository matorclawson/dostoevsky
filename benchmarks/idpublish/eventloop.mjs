// Cross-check the synchronous Python client comparison with two native event-loop clients.
import {createHash} from "node:crypto";
import {spawn} from "node:child_process";
import {createInterface} from "node:readline";
import {once} from "node:events";
import {ENGINES} from "../clients/javascript/adapters.mjs";

async function trial(engine, round, count) {
  const proc = spawn(process.env.SERVE_BIN, ["-engine",engine], {stdio:["pipe","pipe","inherit"]});
  const closed = once(proc,"close");
  let client;
  try {
    const lines = createInterface({input:proc.stdout});
    const [ready] = await once(lines,"line");
    const fields = Object.fromEntries(ready.split(" ").slice(1).map(x=>x.split("=")));
    if (!ready.startsWith("READY ")) throw Error("broker startup");
    client = await ENGINES[engine](fields.addr, fields.token);
    let seq=0;
    for (const workers of round%2?[1,32]:[32,1]) {
      const rows=Array.from({length:count+64},()=>{
        const h=createHash("sha256").update(String(seq++)).digest();
        return {data:Buffer.concat(Array(16).fill(h)),id:h.subarray(0,16).toString("hex")};
      });
      for (const r of rows.slice(0,64)) await client.publish(r.data,r.id);
      let next=64;
      const t0=performance.now();
      await Promise.all(Array.from({length:workers},async()=>{
        while(next<rows.length) {
          const r=rows[next++], start=performance.now();
          r.offset=await client.publish(r.data,r.id);
          r.us=(performance.now()-start)*1000;
        }
      }));
      const rate=count*1000/(performance.now()-t0);
      const timed=rows.slice(64), offsets=new Set();
      for (const r of timed) {
        if(offsets.has(r.offset)||!Buffer.from(await client.read(r.offset)).equals(r.data))throw Error("receipt mismatch");
        offsets.add(r.offset);
      }
      const lat=timed.map(r=>r.us).sort((a,b)=>a-b);
      const q=p=>lat[Math.ceil(count*p)-1].toFixed(1);
      console.log(`engine=${engine} round=${round} workers=${workers} verified=${count} rate=${rate.toFixed(1)} p50_us=${q(.5)} p95_us=${q(.95)} p99_us=${q(.99)}`);
    }
  } finally {
    try { await client?.close(); } finally { proc.stdin.end(); const [code]=await closed;if(code!==0)throw Error("broker shutdown"); }
  }
}
for(let r=1;r<=3;r++)for(const e of r%2?["native","nats"]:["nats","native"])await trial(e,r,2048);
