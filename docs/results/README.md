# Raw trial result lines

One line per trial, as the benchmark printed it. Host names, addresses and paths are not part of these lines. All runs used
one Linux ARM64 server (2 vCPU, ext4) over loopback; the hardware is not identified further.

The SHA-256 beside each file is of the **original, unredacted log of that run, which is held privately**; it is not the digest
of the file published here. For the Rust comparison the lines come from the run's per-trial files, with `arm=` added.

| File | Lines | Run | SHA-256 of the original log |
|---|---:|---|---|
| [2026-11-26-language-clients.log](2026-11-26-language-clients.log) | 240 | language clients over HTTP, source `d7b967ae805b` | `f94a43ce457981f8d76128099a1ebb73caae675d09cfbe2d73a6987fac0201ca` |
| [2026-11-27-rust-nodelay-ab.log](2026-11-27-rust-nodelay-ab.log) | 30 | Rust client before (arm A) and after (arm B) `TCP_NODELAY` | `baae763942642f13ca52f6a45edf7c5260955367123e08435c1d29df82f0d27c` |
| [2026-11-28-native-1-8-32-four-engines.log](2026-11-28-native-1-8-32-four-engines.log) | 60 | native server, 1, 8 and 32 producers, four engines (the README table), source `ed99753f` | `1616eae4841ae1fff9a5a5e5b8ab6bef93f1ffac5bc369461916ce9a7b227efa` |
| [2026-11-28-native-1-32-four-engines.log](2026-11-28-native-1-32-four-engines.log) | 40 | native server, 1 and 32 producers, four engines, source `ed99753f` | `0458fae97a9bbdde5a70488221fb2c4fa71c26943977199761facae70ef565c7` |
| [2026-11-27-native-1-32-three-engines.log](2026-11-27-native-1-32-three-engines.log) | 30 | native server, 1 and 32 producers, without Redis, source `ed99753f` | `38cae3786c094dc64579a4c5a8f9df79606647ad395c60341afd099c781eee8f` |
| [2026-11-27-native-1-8-32-three-engines.log](2026-11-27-native-1-8-32-three-engines.log) | 45 | native server, 1, 8 and 32 producers, without Redis, source `ed99753f` | `a7f926a94ca5c5908cc8cab35c11c161098dc8e13f15a301c533b4242bf4e9f3` |
| [2026-11-27-native-1-32-failed.log](2026-11-27-native-1-32-failed.log) | 6 | **failed run**: stopped at the Redis adapter, partial rows, source `ed99753f` | `4c3d12ef1d53994b7307b584076eb09e3a207d9a3a4bb77588594eb249d0a777` |
| [2026-11-27-native-1-8-32-failed.log](2026-11-27-native-1-8-32-failed.log) | 9 | **failed run**: stopped at the Redis adapter, partial rows, source `ed99753f` | `3501c657085598ad3f3119f413fb2ffb33e09368622e3e4119b3468b21a5c1c6` |

Runs of the native server: Python clients, 512-byte records, one record per request, 2,048 timed messages per cell after 64
warm-up requests, acknowledged after each engine's durable acknowledgment, five rounds with rotated engine order. **The
Dostoevsky server ran with segmented storage, which is opt-in and not the default.** Redis ran without publish deduplication;
the runs that include it use a one-line fix to the benchmark's Redis adapter. `boot_KiB` and `peak_KiB` are the server
process's resident memory when ready and its peak.
