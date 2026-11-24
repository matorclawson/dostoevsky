# Dostoevsky publisher gate on production-03

Both invocations exited 0 against canonical commit 8b5953ac794889d1e151a6f85fdf30855f5d9224, tree 8bd75aee6e12fed4f89d76a6b685670c70785624. Each case recorded all 30 trials and verified 16,000 records per trial. The explicit value ran first. The benchmark wrapper defaults to 2 when GOMAXPROCS is unset, so both runs used the same effective benchmark setting. The second build could reuse the first build's isolated Go caches.

Production-03 ran Linux 7.0.0-34-generic on aarch64, with two ARM Neoverse-N1 vCPUs and TMPDIR on ext4 /dev/sda1 backed by a QEMU virtual disk. Go 1.26.0 linux/arm64 matched the embedded go.mod. HOST.log includes uname, CPU, block devices, mount details and the verified official Go archive digest. [Go distributions](https://go.dev/dl/)

| Input | Producers | Records/sec, Dostoevsky / JetStream | Peak RSS KiB, Dostoevsky / JetStream |
| --- | ---: | ---: | ---: |
| Explicit 2 | 1 | 880 / 746 | 9,532 / 44,216 |
| Explicit 2 | 8 | 2,947 / 823 | 10,152 / 44,200 |
| Explicit 2 | 32 | 7,764 / 815 | 10,300 / 44,664 |
| Unset | 1 | 738 / 668 | 9,660 / 44,160 |
| Unset | 8 | 2,507 / 792 | 10,172 / 44,600 |
| Unset | 32 | 7,826 / 752 | 10,176 / 44,564 |

The raw outputs are explicit-2.log and unset.log. EXIT.log preserves both start and finish times and exit codes. HOST-SHA256SUMS was computed on production-03 and verified again after transfer. Initial cleanup encountered read-only Go module directories. Restoring owner-write permission on those private scratch directories let cleanup finish, as recorded in CLEANUP.log.
