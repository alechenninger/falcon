## Hydration Benchmark Results (Apple M4 Pro, PostgreSQL 18 in container)

Benchmarked hydration time from PostgreSQL at various scales. This measures the startup cost
after a node crash – the time to reload all tuples into memory before serving requests.

### Baseline Performance

| Scale | Tuples | DB I/O | Hydrate | End-to-end | Tuples/sec |
|-------|--------|--------|---------|------------|------------|
| Small | 19K | 4ms | 2ms | 5ms | 3.8M |
| Medium | 237K | 45ms | 21ms | 65ms | 3.6M |
| Large | 2.7M | 717ms | 364ms | 753ms | 3.6M |

### Large Scale Comparison: SELECT vs COPY vs Pipelined (2.7M tuples)

| Method | Time | Tuples/sec | Notes |
|--------|------|------------|-------|
| **Pipelined Batched** | **535ms** | **5.0M** | **FASTEST** - true parallelism |
| SELECT Iterator | 752ms | 3.6M | Serial DB read + hydrate |
| SELECT Batched | 822ms | 3.3M | Pre-loading adds overhead |
| COPY (text) | 821ms | 3.3M | Text parsing offsets protocol gains |
| SELECT DB-only | 502ms | 5.3M | Pure DB read time |
| COPY DB-only | 471ms | 5.7M | 6% faster - protocol is efficient |

**Key findings:**
- **Batched pipelining is 29% faster** by overlapping DB I/O with hydration
- Per-tuple channel pipelining is slower due to channel overhead (~100-200ns/op)
- Batching amortizes channel overhead: ~270 channel ops vs 2.7M
- COPY protocol is 6% faster at DB level but text parsing negates gains

### Optimization Experiments (237K tuples)

| Approach | Time | vs Baseline | Notes |
|----------|------|-------------|-------|
| **Pipelined Batched** | **47.5ms** | **-25%** | **FASTEST** - batch channel ops |
| Baseline (iterator) | 59.4ms | - | Serial: DB read then hydrate |
| Pre-alloc slice | 64.8ms | +9% slower | Extra allocation overhead |
| Pre-alloc slice + HydrateSlice | 63.6ms | +7% slower | Iterator overhead negligible |
| Pre-alloc all (map + slice) | 63.2ms | +6% slower | Map pre-sizing doesn't help |
| Pipelined (per-tuple) | 68.4ms | +15% slower | Channel overhead dominates |
| COPY | ~63ms | +6% slower | Text parsing overhead |

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