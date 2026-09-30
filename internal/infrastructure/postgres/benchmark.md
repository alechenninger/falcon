## Hydration Benchmark Results (Apple M4 Pro, PostgreSQL 18 in container)

Benchmarked hydration time from PostgreSQL at various scales. This measures the startup cost
after a node crash – the time to reload all tuples into memory before serving requests.

### Baseline Performance

| Scale  | Tuples | DB I/O | Hydrate | End-to-end | Tuples/sec |
| ------ | ------ | ------ | ------- | ---------- | ---------- |
| Small  | 19K    | 4ms    | 2ms     | 5ms        | 3.8M       |
| Medium | 237K   | 45ms   | 21ms    | 65ms       | 3.6M       |
| Large  | 2.7M   | 717ms  | 364ms   | 753ms      | 3.6M       |

### Large Scale Comparison: SELECT vs COPY vs Pipelined (2.7M tuples)

| Method                | Time      | Tuples/sec | Notes                               |
| --------------------- | --------- | ---------- | ----------------------------------- |
| **Pipelined Batched** | **535ms** | **5.0M**   | **FASTEST** - true parallelism      |
| SELECT Iterator       | 752ms     | 3.6M       | Serial DB read + hydrate            |
| SELECT Batched        | 822ms     | 3.3M       | Pre-loading adds overhead           |
| COPY (text)           | 821ms     | 3.3M       | Text parsing offsets protocol gains |
| SELECT DB-only        | 502ms     | 5.3M       | Pure DB read time                   |
| COPY DB-only          | 471ms     | 5.7M       | 6% faster - protocol is efficient   |

**Key findings:**

- **Batched pipelining is 29% faster** by overlapping DB I/O with hydration
- Per-tuple channel pipelining is slower due to channel overhead (~100-200ns/op)
- Batching amortizes channel overhead: ~270 channel ops vs 2.7M
- COPY protocol is 6% faster at DB level but text parsing negates gains

### Optimization Experiments (237K tuples)

| Approach                       | Time       | vs Baseline | Notes                           |
| ------------------------------ | ---------- | ----------- | ------------------------------- |
| **Pipelined Batched**          | **47.5ms** | **-25%**    | **FASTEST** - batch channel ops |
| Baseline (iterator)            | 59.4ms     | -           | Serial: DB read then hydrate    |
| Pre-alloc slice                | 64.8ms     | +9% slower  | Extra allocation overhead       |
| Pre-alloc slice + HydrateSlice | 63.6ms     | +7% slower  | Iterator overhead negligible    |
| Pre-alloc all (map + slice)    | 63.2ms     | +6% slower  | Map pre-sizing doesn't help     |
| Pipelined (per-tuple)          | 68.4ms     | +15% slower | Channel overhead dominates      |
| COPY                           | ~63ms      | +6% slower  | Text parsing overhead           |

**Why batched pipelining wins:**

- Per-tuple channel send/receive: ~100-200ns overhead per tuple
- Per-tuple hydration work: ~50ns (map lookup + bitmap add)
- Channel overhead was 2-4x the actual work!
- Batching (10K tuples/batch) reduces channel ops by 10,000x

**Conclusions:**

- **Batched pipelining achieves true parallelism** between DB I/O and hydration
- Per-tuple pipelining fails because channel overhead exceeds work per item
- Pre-allocation doesn't help because the bottleneck was serialization, not allocation
- COPY's protocol efficiency is offset by Go-side text parsing

**Remaining optimization opportunities:**

1. **Parallel connections**: Split load across N connections with `WHERE object_id % N = i`
2. **PostgreSQL tuning**: shared_buffers, effective_cache_size, work_mem
3. **Network optimization**: Unix socket vs TCP for local connections

**Projection at scale:**

- 100M tuple node: ~20 seconds to hydrate (at 5.0M tuples/sec)
- With sharding across 10 nodes: each shard is 10M tuples = ~2 seconds per shard

**Run benchmarks:**

```bash
go test -bench=BenchmarkHydration ./internal/infrastructure/postgres/...
go test -bench=BenchmarkHydrationOptimizations ./internal/infrastructure/postgres/...
go test -bench=BenchmarkLargeSELECTvsCOPY ./internal/infrastructure/postgres/...  # ~60s
```

## Replication Transport Benchmark

Compare transactional `NOTIFY` plus journal fetch, best-effort post-commit `NOTIFY` plus fetch, and a `pgoutput` logical replication slot:

```bash
go test -run '^$' -bench '^BenchmarkReplicationTransport$' -benchtime=10s -benchmem ./internal/infrastructure/postgres
go test -run '^$' -bench '^BenchmarkReplicationTransportAtRate$' -benchtime=30s -count=3 ./internal/infrastructure/postgres
```

`BenchmarkReplicationTransport` is a saturation comparison with 16 writers. `BenchmarkReplicationTransportAtRate` uses equal offered load for every transport: one writer at 100 writes/sec, and 16 writers at 1,000 writes/sec total. The 16-writer scenario runs with both workloads: one 256-byte row per transaction or 16 1-KiB rows. Notification variants are tested with fetch limits of 1 and 128; the slot consumer applies complete `pgoutput` transactions, while notification consumers fetch complete batches from the indexed table. Scheduler lag reports whether writers fall behind the offered rate. Polling fallback is intentionally excluded.

The local setup uses PostgreSQL 18 Alpine in a Testcontainer capped at 4 GiB. It enables `wal_level=logical`, sets `shared_buffers=1GB`, `effective_cache_size=3GB` (a planner estimate, not allocated memory), `work_mem=4MB`, and `max_wal_size=4GB`, and leaves `fsync` and `synchronous_commit` at PostgreSQL defaults. Before timing, the benchmark waits for the notification listener or replication stream to be ready and warms pooled fetch connections with an empty indexed query. Timing includes consumer drain. Each run uses unique table, channel, publication, and slot names and drops them afterward.

Metrics include committed writes/sec, fully applied batches/sec, payload MiB/sec, p50/p99 operation-to-apply and commit-to-apply latency, commit duration, and remaining catch-up time per transaction (`max(0, apply time - commit finish)`). Remaining catch-up percentiles are calculated from per-transaction values; applied-before-commit percentage is reported separately. Notification runs also report average and p99 transactions per fetch. Consumers validate row counts and payload checksums, and duplicate batch application fails the run. This measures transport and decoding without including Falcon graph mutation costs.

For measurements at deployment RTT, point the benchmark at a dedicated external database from the same network location as the application:

```bash
FALCON_REPLICATION_BENCH_DATABASE_URL="$BENCH_DATABASE_URL" go test -run '^$' -bench '^BenchmarkReplicationTransportAtRate$' -benchtime=30s -count=3 ./internal/infrastructure/postgres
```

When `FALCON_REPLICATION_BENCH_DATABASE_URL` is set, no container is started and the external server's PostgreSQL settings are used unchanged. The server must support logical replication (`wal_level=logical`, at least one replication slot and WAL sender available); the supplied role needs permission to create tables/publications and logical replication slots. Use a dedicated database, since the benchmark creates and drops temporary objects. An interrupted process can leave those objects behind. The benchmark logs the server's memory settings; unlike the local setup, it does not impose a memory profile.

PostgreSQL delivers transactional notifications only after commit, and the writer does not wait for a listener to process them. However, queue exhaustion can make the transaction fail at commit. `NotifyAfterCommit` measures the alternative extra notification round trip, but it is not a reliable standalone protocol: a process crash between data commit and notification loses the wakeup. It needs a durable reconciliation path in production. The benchmark measures performance, not notification-loss recovery or queue-exhaustion behavior.

Use `-benchtime=1x` only as a wiring check. Use repeated sustained runs at the intended database topology for performance decisions. The local memory profile is a useful approximation, not a full RDS model for CPU, storage, or network limits; container-local results do not predict managed-service RTT or behavior.
