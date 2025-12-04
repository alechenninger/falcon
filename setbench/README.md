# Falcon - Set Membership Benchmark

Compares memory and CPU overhead of two set membership check approaches using **opaque string IDs** (UUID-format):

1. **Roaring Bitmap + Mapping**: Uses a dictionary to map arbitrary string IDs to dense uint32 integers, then uses roaring bitmaps for membership checks.

2. **Plain Set**: Uses a standard Go `map[string]struct{}` hash set.

## Running Benchmarks

```bash
# Run all benchmarks with memory stats
go test -bench=. -benchmem

# Run with more iterations for stability
go test -bench=. -benchmem -count=5

# Run specific benchmark categories
go test -bench=BenchmarkConstruction -benchmem
go test -bench=BenchmarkLookup -benchmem
go test -bench=BenchmarkMemoryUsage -benchmem
go test -bench=BenchmarkSingleLookup -benchmem
go test -bench=BenchmarkSingleAdd -benchmem
go test -bench=BenchmarkMultiSet -benchmem

# Save results for comparison with benchstat
go test -bench=. -benchmem -count=10 > results.txt
```

## Benchmark Categories

| Benchmark | What it measures |
|-----------|------------------|
| `BenchmarkConstruction` | Time to build a set from scratch |
| `BenchmarkLookup` | Time to perform 10k lookups with varying hit rates |
| `BenchmarkMemoryUsage` | Steady-state heap memory usage |
| `BenchmarkSingleLookup` | Per-operation lookup cost |
| `BenchmarkSingleAdd` | Per-operation add cost |
| `BenchmarkMultiSetFindFirst` | Check N sets until finding a member (shared IDs) |
| `BenchmarkMultiSetMemory` | Memory usage with many sets sharing IDs |
| `BenchmarkMultiSetVaryMembership` | Effect of membership density on lookup |

## Test Data

- **IDs**: UUID-format strings (36 characters, e.g., `a1b2c3d4-e5f6-7890-abcd-ef1234567890`)
- **Scales**: 1K, 10K, 100K, and 1M elements

## Interpreting Results

- **ns/op**: CPU time per operation
- **B/op**: Bytes allocated per operation
- **allocs/op**: Number of allocations per operation
- **heap-bytes**: Total heap memory used (in memory benchmarks)

---

## Single Set Results

With a single set and string IDs, **PlainSet wins** but the gap is smaller than with integer keys:

| Operation | Roaring + Mapping | Plain Set | Ratio |
|-----------|-------------------|-----------|-------|
| Single Lookup | ~7 ns | ~5 ns | 1.4x |
| Single Add | ~7 ns | ~5 ns | 1.4x |
| Memory @ 1M | ~56 MB | ~56 MB | 1.0x |

The gap is smaller because string hashing is expensive and both approaches pay that cost. Memory is similar because the string keys dominate—each 36-byte UUID plus map overhead.

---

## Multi-Set Scenario: Shared IDs Across Many Sets

When IDs are reused across many sets (e.g., user IDs across group memberships), we can share a **single dictionary** across all roaring bitmaps. With string IDs, this is even more impactful.

### The Crossover Points

**Memory** — Roaring wins dramatically with strings:

| Sets | IDs | SharedRoaring | PlainSet | Savings |
|------|-----|---------------|----------|---------|
| 10 | 10K | 945 KB | 861 KB | ~1x |
| 100 | 10K | 1.2 MB | 5.9 MB | **5x** |
| 1000 | 10K | 3.8 MB | 55 MB | **15x** |
| 100 | 100K | 10 MB | 48.5 MB | **5x** |

With strings, the savings are **amplified** because:
- PlainSet stores the full string key in *every* set's map
- SharedRoaring stores each string only *once* in the shared dictionary

**Lookup Speed** (find first matching set) — Crossover around 100 sets:

| Sets | SharedRoaring | PlainSet | Winner |
|------|---------------|----------|--------|
| 2 | 29 µs | 15 µs | PlainSet (1.9x) |
| 10 | 109 µs | 61 µs | PlainSet (1.8x) |
| 50 | 274 µs | 222 µs | PlainSet (1.2x) |
| 100 | 415 µs | 419 µs | **~Tie** |
| 200 | 733 µs | 837 µs | **Roaring (1.1x)** |
| 500 | 1.6 ms | 2.9 ms | **Roaring (1.9x)** |
| 1000 | 3.0 ms | 10.6 ms | **Roaring (3.5x)** |

### Why Roaring Wins at Scale

1. **String deduplication**: One copy of each string vs N copies (one per set)
2. **Compact bitmaps**: Each roaring bitmap is tiny (~KB) vs hash map (~MB)
3. **Cache efficiency**: Bitmaps are dense; scattered string pointers thrash cache
4. **Scales with sets, not IDs**: Adding sets costs bitmap bytes, not string storage

### Recommendation

| Scenario | Use |
|----------|-----|
| 1 set, arbitrary IDs | PlainSet |
| Few sets (<50), need speed | PlainSet |
| Many sets (100+) sharing IDs | SharedRoaring |
| Memory-constrained with 10+ sets | SharedRoaring |
| Need set operations (union/intersection) | SharedRoaring |

### When Roaring Always Wins

- **Dense integer IDs**: Use roaring directly without a dictionary
- **Set operations**: Union/intersection/difference are O(n) vs O(n²)
- **Serialization**: Roaring is much more compact on disk/wire
- **Range queries**: Roaring supports efficient range iteration
