package falcon

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"runtime"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
)

// Benchmark parameters
var benchSizes = []int{
	1_000,
	10_000,
	100_000,
	1_000_000,
}

// generateUUIDs creates n UUID-like string IDs.
// These are 36-character strings in the format xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
func generateUUIDs(n int, seed int64) []string {
	rng := rand.New(rand.NewSource(seed))
	ids := make([]string, n)
	buf := make([]byte, 16)

	for i := range ids {
		rng.Read(buf)
		ids[i] = fmt.Sprintf("%s-%s-%s-%s-%s",
			hex.EncodeToString(buf[0:4]),
			hex.EncodeToString(buf[4:6]),
			hex.EncodeToString(buf[6:8]),
			hex.EncodeToString(buf[8:10]),
			hex.EncodeToString(buf[10:16]),
		)
	}
	return ids
}

// generateLookupIDs creates a mix of IDs for lookup testing:
// - hitRatio of them will be from the existing set (hits)
// - The rest will be random IDs unlikely to be in the set (misses)
func generateLookupIDs(existingIDs []string, n int, hitRatio float64, seed int64) []string {
	rng := rand.New(rand.NewSource(seed))
	lookups := make([]string, n)
	numHits := int(float64(n) * hitRatio)

	for i := 0; i < numHits; i++ {
		lookups[i] = existingIDs[rng.Intn(len(existingIDs))]
	}

	// Generate random UUIDs for misses
	buf := make([]byte, 16)
	for i := numHits; i < n; i++ {
		rng.Read(buf)
		lookups[i] = fmt.Sprintf("%s-%s-%s-%s-%s",
			hex.EncodeToString(buf[0:4]),
			hex.EncodeToString(buf[4:6]),
			hex.EncodeToString(buf[6:8]),
			hex.EncodeToString(buf[8:10]),
			hex.EncodeToString(buf[10:16]),
		)
	}

	// Shuffle to interleave hits and misses
	rng.Shuffle(len(lookups), func(i, j int) {
		lookups[i], lookups[j] = lookups[j], lookups[i]
	})

	return lookups
}

// ============================================================================
// Construction Benchmarks
// ============================================================================

func BenchmarkConstruction(b *testing.B) {
	for _, size := range benchSizes {
		ids := generateUUIDs(size, 42)

		b.Run(fmt.Sprintf("Roaring/n=%d", size), func(b *testing.B) {
			for b.Loop() {
				set := NewRoaringSetWithCapacity(size)
				for _, id := range ids {
					set.Add(id)
				}
			}
		})

		b.Run(fmt.Sprintf("PlainSet/n=%d", size), func(b *testing.B) {
			for b.Loop() {
				set := NewPlainSetWithCapacity(size)
				for _, id := range ids {
					set.Add(id)
				}
			}
		})
	}
}

// ============================================================================
// Lookup Benchmarks
// ============================================================================

func BenchmarkLookup(b *testing.B) {
	// Test with different hit ratios to see how cache behavior differs
	hitRatios := []float64{0.5, 0.9, 0.1}

	for _, size := range benchSizes {
		ids := generateUUIDs(size, 42)

		// Pre-build the sets
		roaringSet := NewRoaringSetWithCapacity(size)
		plainSet := NewPlainSetWithCapacity(size)
		for _, id := range ids {
			roaringSet.Add(id)
			plainSet.Add(id)
		}

		for _, hitRatio := range hitRatios {
			lookups := generateLookupIDs(ids, 10000, hitRatio, 123)

			b.Run(fmt.Sprintf("Roaring/n=%d/hit=%.0f%%", size, hitRatio*100), func(b *testing.B) {
				for b.Loop() {
					for _, id := range lookups {
						_ = roaringSet.Contains(id)
					}
				}
			})

			b.Run(fmt.Sprintf("PlainSet/n=%d/hit=%.0f%%", size, hitRatio*100), func(b *testing.B) {
				for b.Loop() {
					for _, id := range lookups {
						_ = plainSet.Contains(id)
					}
				}
			})
		}
	}
}

// ============================================================================
// Memory Usage Measurement
// ============================================================================

// measureMemory forces GC and returns allocated heap bytes.
func measureMemory() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// sliceSink prevents the compiler from optimizing away slice allocations.
var sliceSink []uint64

// BenchmarkMemoryUsage measures steady-state memory usage of each approach.
// This is reported separately from the construction benchmark to get a clearer
// picture of the actual memory footprint.
func BenchmarkMemoryUsage(b *testing.B) {
	for _, size := range benchSizes {
		ids := generateUUIDs(size, 42)

		b.Run(fmt.Sprintf("Roaring/n=%d", size), func(b *testing.B) {
			for b.Loop() {
				before := measureMemory()
				set := NewRoaringSetWithCapacity(size)
				for _, id := range ids {
					set.Add(id)
				}
				after := measureMemory()

				b.ReportMetric(float64(after-before), "heap-bytes")

				// Prevent optimization
				if set.Cardinality() == 0 {
					b.Fatal("unexpected empty set")
				}
			}
		})

		b.Run(fmt.Sprintf("PlainSet/n=%d", size), func(b *testing.B) {
			for b.Loop() {
				before := measureMemory()
				set := NewPlainSetWithCapacity(size)
				for _, id := range ids {
					set.Add(id)
				}
				after := measureMemory()

				b.ReportMetric(float64(after-before), "heap-bytes")

				// Prevent optimization
				if set.Cardinality() == 0 {
					b.Fatal("unexpected empty set")
				}
			}
		})
	}
}

// ============================================================================
// Single Lookup Benchmarks (for per-operation cost)
// ============================================================================

func BenchmarkSingleLookup(b *testing.B) {
	// Use a large set to see steady-state behavior
	size := 1_000_000
	ids := generateUUIDs(size, 42)

	roaringSet := NewRoaringSetWithCapacity(size)
	plainSet := NewPlainSetWithCapacity(size)
	for _, id := range ids {
		roaringSet.Add(id)
		plainSet.Add(id)
	}

	// Pre-generate enough lookup targets
	lookups := generateLookupIDs(ids, 10_000_000, 0.5, 999)

	b.Run("Roaring", func(b *testing.B) {
		i := 0
		for b.Loop() {
			_ = roaringSet.Contains(lookups[i%len(lookups)])
			i++
		}
	})

	b.Run("PlainSet", func(b *testing.B) {
		i := 0
		for b.Loop() {
			_ = plainSet.Contains(lookups[i%len(lookups)])
			i++
		}
	})
}

// ============================================================================
// Single Add Benchmarks
// ============================================================================

func BenchmarkSingleAdd(b *testing.B) {
	// Pre-generate enough IDs
	ids := generateUUIDs(10_000_000, 42)

	b.Run("Roaring", func(b *testing.B) {
		set := NewRoaringSet()
		i := 0
		for b.Loop() {
			set.Add(ids[i%len(ids)])
			i++
		}
	})

	b.Run("PlainSet", func(b *testing.B) {
		set := NewPlainSet()
		i := 0
		for b.Loop() {
			set.Add(ids[i%len(ids)])
			i++
		}
	})
}

// ============================================================================
// Multi-Set Benchmarks: Shared IDs across many sets
// ============================================================================

// multiSetConfig defines a scenario for multi-set benchmarks.
type multiSetConfig struct {
	numSets       int     // Number of sets
	numIDs        int     // Total unique IDs in the shared dictionary
	membershipPct float64 // Probability each ID appears in each set
}

// buildMultiSetData creates the test data for multi-set benchmarks.
// Returns:
//   - sharedRoaringSets: sets using shared dictionary
//   - plainSets: independent hash sets
//   - allIDs: the universe of IDs
//   - dict: the shared dictionary (for memory measurement)
func buildMultiSetData(cfg multiSetConfig, seed int64) (
	[]*SharedRoaringSet, []*PlainSet, []string, *IDDictionary,
) {
	rng := rand.New(rand.NewSource(seed))

	// Generate the shared ID universe
	allIDs := generateUUIDs(cfg.numIDs, seed)

	// Create shared dictionary
	dict := NewIDDictionaryWithCapacity(cfg.numIDs)
	for _, id := range allIDs {
		dict.GetOrAssign(id)
	}

	// Create sets
	sharedRoaringSets := make([]*SharedRoaringSet, cfg.numSets)
	plainSets := make([]*PlainSet, cfg.numSets)

	for i := 0; i < cfg.numSets; i++ {
		sharedRoaringSets[i] = NewSharedRoaringSet(dict)
		plainSets[i] = NewPlainSet()

		// Each ID has membershipPct chance of being in this set
		for _, id := range allIDs {
			if rng.Float64() < cfg.membershipPct {
				sharedRoaringSets[i].Add(id)
				plainSets[i].Add(id)
			}
		}
	}

	return sharedRoaringSets, plainSets, allIDs, dict
}

// findFirstContaining searches sets until it finds one containing the ID.
// Returns the index of the first matching set, or -1 if none found.
func findFirstContainingRoaring(sets []*SharedRoaringSet, id string) int {
	for i, s := range sets {
		if s.Contains(id) {
			return i
		}
	}
	return -1
}

func findFirstContainingPlain(sets []*PlainSet, id string) int {
	for i, s := range sets {
		if s.Contains(id) {
			return i
		}
	}
	return -1
}

// BenchmarkMultiSetFindFirst tests "find first matching set" with varying
// numbers of sets and ID reuse. This is the key benchmark to find the
// crossover point where shared-dictionary roaring beats plain sets.
func BenchmarkMultiSetFindFirst(b *testing.B) {
	// Test configurations: vary number of sets to find crossover
	configs := []multiSetConfig{
		// Few sets - plain should win
		{numSets: 2, numIDs: 10_000, membershipPct: 0.1},
		{numSets: 5, numIDs: 10_000, membershipPct: 0.1},
		{numSets: 10, numIDs: 10_000, membershipPct: 0.1},

		// More sets - crossover region
		{numSets: 20, numIDs: 10_000, membershipPct: 0.1},
		{numSets: 50, numIDs: 10_000, membershipPct: 0.1},
		{numSets: 100, numIDs: 10_000, membershipPct: 0.1},

		// Many sets - roaring should win
		{numSets: 200, numIDs: 10_000, membershipPct: 0.1},
		{numSets: 500, numIDs: 10_000, membershipPct: 0.1},
		{numSets: 1000, numIDs: 10_000, membershipPct: 0.1},
	}

	for _, cfg := range configs {
		roaringSets, plainSets, allIDs, _ := buildMultiSetData(cfg, 42)

		// Generate lookup IDs (use IDs from the universe, ~50% should hit somewhere)
		lookupIDs := generateLookupIDs(allIDs, 1000, 0.5, 123)

		name := fmt.Sprintf("sets=%d/ids=%d/mem=%.0f%%", cfg.numSets, cfg.numIDs, cfg.membershipPct*100)

		b.Run("SharedRoaring/"+name, func(b *testing.B) {
			for b.Loop() {
				for _, id := range lookupIDs {
					_ = findFirstContainingRoaring(roaringSets, id)
				}
			}
		})

		b.Run("PlainSet/"+name, func(b *testing.B) {
			for b.Loop() {
				for _, id := range lookupIDs {
					_ = findFirstContainingPlain(plainSets, id)
				}
			}
		})
	}
}

// BenchmarkMultiSetMemory measures memory usage with many sets sharing IDs.
// This shows the memory advantage of shared dictionary + roaring bitmaps.
func BenchmarkMultiSetMemory(b *testing.B) {
	configs := []multiSetConfig{
		{numSets: 10, numIDs: 10_000, membershipPct: 0.1},
		{numSets: 100, numIDs: 10_000, membershipPct: 0.1},
		{numSets: 1000, numIDs: 10_000, membershipPct: 0.1},
		{numSets: 100, numIDs: 100_000, membershipPct: 0.1},
	}

	for _, cfg := range configs {
		name := fmt.Sprintf("sets=%d/ids=%d/mem=%.0f%%", cfg.numSets, cfg.numIDs, cfg.membershipPct*100)

		b.Run("SharedRoaring/"+name, func(b *testing.B) {
			for b.Loop() {
				before := measureMemory()
				roaringSets, _, _, dict := buildMultiSetData(cfg, 42)
				after := measureMemory()

				b.ReportMetric(float64(after-before), "heap-bytes")

				// Prevent optimization
				total := uint64(dict.Len())
				for _, s := range roaringSets {
					total += s.Cardinality()
				}
				if total == 0 {
					b.Fatal("unexpected")
				}
			}
		})

		b.Run("PlainSet/"+name, func(b *testing.B) {
			for b.Loop() {
				before := measureMemory()
				_, plainSets, _, _ := buildMultiSetData(cfg, 42)
				after := measureMemory()

				b.ReportMetric(float64(after-before), "heap-bytes")

				// Prevent optimization
				total := uint64(0)
				for _, s := range plainSets {
					total += s.Cardinality()
				}
				if total == 0 {
					b.Fatal("unexpected")
				}
			}
		})
	}
}

// ============================================================================
// Clone Benchmarks
// ============================================================================

// BenchmarkRoaringClone measures the cost of cloning roaring bitmaps of different sizes.
func BenchmarkRoaringClone(b *testing.B) {
	for _, size := range benchSizes {
		// Build a bitmap with 'size' elements
		bitmap := roaring.NewBitmap()
		for i := uint32(0); i < uint32(size); i++ {
			bitmap.Add(i)
		}

		b.Run(fmt.Sprintf("n=%d", size), func(b *testing.B) {
			for b.Loop() {
				_ = bitmap.Clone()
			}
		})
	}
}

// BenchmarkRoaringCloneSparse measures clone cost for sparse bitmaps (worst case compression).
// Sparse bitmaps have values spread across the 32-bit range, reducing run-length compression.
func BenchmarkRoaringCloneSparse(b *testing.B) {
	for _, size := range benchSizes {
		rng := rand.New(rand.NewSource(int64(size)))
		bitmap := roaring.NewBitmap()
		for range size {
			// Add random values spread across the uint32 range
			bitmap.Add(rng.Uint32())
		}

		b.Run(fmt.Sprintf("n=%d", size), func(b *testing.B) {
			for b.Loop() {
				_ = bitmap.Clone()
			}
		})
	}
}

// ============================================================================
// Map Value Size Comparison Benchmarks
// ============================================================================

// ShardedID represents a shard ID (uint16) and a local ID (uint32).
// On a 64-bit machine, this struct will be 8 bytes due to alignment
// (2 bytes for ShardID + 2 bytes padding + 4 bytes for ID).
type ShardedID struct {
	ShardID uint16
	ID      uint32
}

// BenchmarkMapValueMemory measures memory usage for maps with different value types.
// This tests whether a struct of uint16+uint32 takes less RAM than uint64.
func BenchmarkMapValueMemory(b *testing.B) {
	for _, size := range benchSizes {
		ids := generateUUIDs(size, 42)

		b.Run(fmt.Sprintf("StringToUint32/n=%d", size), func(b *testing.B) {
			for b.Loop() {
				before := measureMemory()
				m := make(map[string]uint32, size)
				for i, id := range ids {
					m[id] = uint32(i)
				}
				after := measureMemory()

				b.ReportMetric(float64(after-before), "heap-bytes")
				b.ReportMetric(float64(after-before)/float64(size), "bytes/entry")

				// Prevent optimization
				if len(m) == 0 {
					b.Fatal("unexpected empty map")
				}
			}
		})

		b.Run(fmt.Sprintf("StringToUint64/n=%d", size), func(b *testing.B) {
			for b.Loop() {
				before := measureMemory()
				m := make(map[string]uint64, size)
				for i, id := range ids {
					m[id] = uint64(i)
				}
				after := measureMemory()

				b.ReportMetric(float64(after-before), "heap-bytes")
				b.ReportMetric(float64(after-before)/float64(size), "bytes/entry")

				// Prevent optimization
				if len(m) == 0 {
					b.Fatal("unexpected empty map")
				}
			}
		})

		b.Run(fmt.Sprintf("StringToShardedID/n=%d", size), func(b *testing.B) {
			for b.Loop() {
				before := measureMemory()
				m := make(map[string]ShardedID, size)
				for i, id := range ids {
					m[id] = ShardedID{
						ShardID: uint16(i % 1000), // Simulate 1000 shards
						ID:      uint32(i),
					}
				}
				after := measureMemory()

				b.ReportMetric(float64(after-before), "heap-bytes")
				b.ReportMetric(float64(after-before)/float64(size), "bytes/entry")

				// Prevent optimization
				if len(m) == 0 {
					b.Fatal("unexpected empty map")
				}
			}
		})
	}
}

// ============================================================================
// Grouped Bitmap Memory Benchmarks
// ============================================================================

// BenchmarkGroupedBitmapMemory compares memory usage of different bitmap grouping strategies.
// This simulates storing sets for objects where IDs have a hierarchical structure:
// - Flat: Single bitmap with 32-bit IDs (ideal case, no grouping overhead)
// - GroupedBy32bit: map[uint32]*roaring.Bitmap (e.g., 64-bit IDs = 32-bit object + 32-bit local)
// - GroupedBy16bit: map[uint16]*roaring.Bitmap (e.g., 48-bit IDs = 16-bit shard + 32-bit local)
func BenchmarkGroupedBitmapMemory(b *testing.B) {
	type config struct {
		totalIDs  int
		numGroups int
	}

	configs := []config{
		// Vary total IDs with fixed group count
		{totalIDs: 10_000, numGroups: 100},
		{totalIDs: 100_000, numGroups: 100},
		{totalIDs: 1_000_000, numGroups: 100},

		// Vary group count with fixed total IDs
		{totalIDs: 100_000, numGroups: 10},
		{totalIDs: 100_000, numGroups: 100},
		{totalIDs: 100_000, numGroups: 1000},
		{totalIDs: 100_000, numGroups: 10_000},
	}

	for _, cfg := range configs {
		name := fmt.Sprintf("ids=%d/groups=%d", cfg.totalIDs, cfg.numGroups)

		b.Run("Flat/"+name, func(b *testing.B) {
			for b.Loop() {
				before := measureMemory()
				bitmap := roaring.NewBitmap()
				for i := uint32(0); i < uint32(cfg.totalIDs); i++ {
					bitmap.Add(i)
				}
				after := measureMemory()

				b.ReportMetric(float64(after-before), "heap-bytes")
				b.ReportMetric(float64(after-before)/float64(cfg.totalIDs), "bytes/id")

				if bitmap.GetCardinality() == 0 {
					b.Fatal("unexpected empty bitmap")
				}
			}
		})

		b.Run("GroupedBy32bit/"+name, func(b *testing.B) {
			for b.Loop() {
				before := measureMemory()
				groups := make(map[uint32]*roaring.Bitmap, cfg.numGroups)
				for i := 0; i < cfg.totalIDs; i++ {
					groupID := uint32(i % cfg.numGroups)
					localID := uint32(i / cfg.numGroups)
					bm, ok := groups[groupID]
					if !ok {
						bm = roaring.NewBitmap()
						groups[groupID] = bm
					}
					bm.Add(localID)
				}
				after := measureMemory()

				b.ReportMetric(float64(after-before), "heap-bytes")
				b.ReportMetric(float64(after-before)/float64(cfg.totalIDs), "bytes/id")

				// Prevent optimization
				total := uint64(0)
				for _, bm := range groups {
					total += bm.GetCardinality()
				}
				if total == 0 {
					b.Fatal("unexpected empty")
				}
			}
		})

		b.Run("GroupedBy16bit/"+name, func(b *testing.B) {
			// Cap groups at uint16 max
			effectiveGroups := cfg.numGroups
			if effectiveGroups > 65535 {
				effectiveGroups = 65535
			}

			for b.Loop() {
				before := measureMemory()
				groups := make(map[uint16]*roaring.Bitmap, effectiveGroups)
				for i := 0; i < cfg.totalIDs; i++ {
					groupID := uint16(i % effectiveGroups)
					localID := uint32(i / effectiveGroups)
					bm, ok := groups[groupID]
					if !ok {
						bm = roaring.NewBitmap()
						groups[groupID] = bm
					}
					bm.Add(localID)
				}
				after := measureMemory()

				b.ReportMetric(float64(after-before), "heap-bytes")
				b.ReportMetric(float64(after-before)/float64(cfg.totalIDs), "bytes/id")

				// Prevent optimization
				total := uint64(0)
				for _, bm := range groups {
					total += bm.GetCardinality()
				}
				if total == 0 {
					b.Fatal("unexpected empty")
				}
			}
		})

		b.Run("Roaring64/"+name, func(b *testing.B) {
			for b.Loop() {
				before := measureMemory()
				bitmap := roaring64.NewBitmap()
				for i := 0; i < cfg.totalIDs; i++ {
					// Construct 64-bit ID: high 32 bits = group, low 32 bits = local ID
					groupID := uint64(i % cfg.numGroups)
					localID := uint64(i / cfg.numGroups)
					id64 := (groupID << 32) | localID
					bitmap.Add(id64)
				}
				after := measureMemory()

				b.ReportMetric(float64(after-before), "heap-bytes")
				b.ReportMetric(float64(after-before)/float64(cfg.totalIDs), "bytes/id")

				if bitmap.GetCardinality() == 0 {
					b.Fatal("unexpected empty bitmap")
				}
			}
		})

		b.Run("PlainMap64/"+name, func(b *testing.B) {
			for b.Loop() {
				before := measureMemory()
				m := make(map[uint64]struct{}, cfg.totalIDs)
				for i := 0; i < cfg.totalIDs; i++ {
					// Construct 64-bit ID: high 32 bits = group, low 32 bits = local ID
					groupID := uint64(i % cfg.numGroups)
					localID := uint64(i / cfg.numGroups)
					id64 := (groupID << 32) | localID
					m[id64] = struct{}{}
				}
				after := measureMemory()

				b.ReportMetric(float64(after-before), "heap-bytes")
				b.ReportMetric(float64(after-before)/float64(cfg.totalIDs), "bytes/id")

				if len(m) == 0 {
					b.Fatal("unexpected empty map")
				}
			}
		})

		b.Run("Slice64/"+name, func(b *testing.B) {
			for b.Loop() {
				// Clear previous allocation and force GC
				sliceSink = nil
				before := measureMemory()

				s := make([]uint64, cfg.totalIDs)
				for i := 0; i < cfg.totalIDs; i++ {
					// Construct 64-bit ID: high 32 bits = group, low 32 bits = local ID
					groupID := uint64(i % cfg.numGroups)
					localID := uint64(i / cfg.numGroups)
					id64 := (groupID << 32) | localID
					s[i] = id64
				}
				// Escape to heap via package-level sink
				sliceSink = s
				after := measureMemory()

				b.ReportMetric(float64(after-before), "heap-bytes")
				b.ReportMetric(float64(after-before)/float64(cfg.totalIDs), "bytes/id")

				if len(s) == 0 {
					b.Fatal("unexpected empty slice")
				}
			}
		})
	}
}

// BenchmarkGroupedBitmapSparse tests with sparse/random IDs (less compressible).
// This is more realistic for real-world ID distributions.
func BenchmarkGroupedBitmapSparse(b *testing.B) {
	type config struct {
		totalIDs  int
		numGroups int
	}

	configs := []config{
		{totalIDs: 100_000, numGroups: 100},
		{totalIDs: 100_000, numGroups: 1000},
		{totalIDs: 1_000_000, numGroups: 100},
		{totalIDs: 1_000_000, numGroups: 1000},
	}

	for _, cfg := range configs {
		name := fmt.Sprintf("ids=%d/groups=%d", cfg.totalIDs, cfg.numGroups)
		rng := rand.New(rand.NewSource(42))

		// Pre-generate random IDs for each group
		type idPair struct {
			group   int
			localID uint32
		}
		pairs := make([]idPair, cfg.totalIDs)
		for i := range pairs {
			pairs[i] = idPair{
				group:   rng.Intn(cfg.numGroups),
				localID: rng.Uint32(), // Random 32-bit ID (sparse)
			}
		}

		b.Run("Flat/"+name, func(b *testing.B) {
			for b.Loop() {
				before := measureMemory()
				bitmap := roaring.NewBitmap()
				for _, p := range pairs {
					// For flat, we just use the local ID directly
					bitmap.Add(p.localID)
				}
				after := measureMemory()

				b.ReportMetric(float64(after-before), "heap-bytes")
				b.ReportMetric(float64(after-before)/float64(cfg.totalIDs), "bytes/id")

				if bitmap.GetCardinality() == 0 {
					b.Fatal("unexpected empty bitmap")
				}
			}
		})

		b.Run("GroupedBy32bit/"+name, func(b *testing.B) {
			for b.Loop() {
				before := measureMemory()
				groups := make(map[uint32]*roaring.Bitmap, cfg.numGroups)
				for _, p := range pairs {
					groupID := uint32(p.group)
					bm, ok := groups[groupID]
					if !ok {
						bm = roaring.NewBitmap()
						groups[groupID] = bm
					}
					bm.Add(p.localID)
				}
				after := measureMemory()

				b.ReportMetric(float64(after-before), "heap-bytes")
				b.ReportMetric(float64(after-before)/float64(cfg.totalIDs), "bytes/id")

				total := uint64(0)
				for _, bm := range groups {
					total += bm.GetCardinality()
				}
				if total == 0 {
					b.Fatal("unexpected empty")
				}
			}
		})

		b.Run("GroupedBy16bit/"+name, func(b *testing.B) {
			for b.Loop() {
				before := measureMemory()
				groups := make(map[uint16]*roaring.Bitmap, cfg.numGroups)
				for _, p := range pairs {
					groupID := uint16(p.group)
					bm, ok := groups[groupID]
					if !ok {
						bm = roaring.NewBitmap()
						groups[groupID] = bm
					}
					bm.Add(p.localID)
				}
				after := measureMemory()

				b.ReportMetric(float64(after-before), "heap-bytes")
				b.ReportMetric(float64(after-before)/float64(cfg.totalIDs), "bytes/id")

				total := uint64(0)
				for _, bm := range groups {
					total += bm.GetCardinality()
				}
				if total == 0 {
					b.Fatal("unexpected empty")
				}
			}
		})

		b.Run("Roaring64/"+name, func(b *testing.B) {
			for b.Loop() {
				before := measureMemory()
				bitmap := roaring64.NewBitmap()
				for _, p := range pairs {
					// Construct 64-bit ID: high 32 bits = group, low 32 bits = local ID
					id64 := (uint64(p.group) << 32) | uint64(p.localID)
					bitmap.Add(id64)
				}
				after := measureMemory()

				b.ReportMetric(float64(after-before), "heap-bytes")
				b.ReportMetric(float64(after-before)/float64(cfg.totalIDs), "bytes/id")

				if bitmap.GetCardinality() == 0 {
					b.Fatal("unexpected empty bitmap")
				}
			}
		})

		b.Run("PlainMap64/"+name, func(b *testing.B) {
			for b.Loop() {
				before := measureMemory()
				m := make(map[uint64]struct{}, cfg.totalIDs)
				for _, p := range pairs {
					// Construct 64-bit ID: high 32 bits = group, low 32 bits = local ID
					id64 := (uint64(p.group) << 32) | uint64(p.localID)
					m[id64] = struct{}{}
				}
				after := measureMemory()

				b.ReportMetric(float64(after-before), "heap-bytes")
				b.ReportMetric(float64(after-before)/float64(cfg.totalIDs), "bytes/id")

				if len(m) == 0 {
					b.Fatal("unexpected empty map")
				}
			}
		})

		b.Run("Slice64/"+name, func(b *testing.B) {
			for b.Loop() {
				// Clear previous allocation and force GC
				sliceSink = nil
				before := measureMemory()

				s := make([]uint64, cfg.totalIDs)
				for i, p := range pairs {
					// Construct 64-bit ID: high 32 bits = group, low 32 bits = local ID
					id64 := (uint64(p.group) << 32) | uint64(p.localID)
					s[i] = id64
				}
				// Escape to heap via package-level sink
				sliceSink = s
				after := measureMemory()

				b.ReportMetric(float64(after-before), "heap-bytes")
				b.ReportMetric(float64(after-before)/float64(cfg.totalIDs), "bytes/id")

				if len(s) == 0 {
					b.Fatal("unexpected empty slice")
				}
			}
		})
	}
}

// ============================================================================
// Integer ID Lookup Benchmarks
// ============================================================================

// sliceContains performs linear search on a uint64 slice.
func sliceContains(s []uint64, target uint64) bool {
	for _, v := range s {
		if v == target {
			return true
		}
	}
	return false
}

// BenchmarkIntegerLookup compares lookup performance for integer IDs across:
// - Roaring32 bitmap (sparse 32-bit IDs)
// - Roaring64 bitmap
// - map[uint64]struct{}
// - []uint64 slice (linear search)
func BenchmarkIntegerLookup(b *testing.B) {
	sizes := []int{100, 1000, 10_000, 100_000}

	for _, size := range sizes {
		// Generate random 32-bit and 64-bit IDs
		rng := rand.New(rand.NewSource(42))
		ids32 := make([]uint32, size)
		ids64 := make([]uint64, size)
		for i := range ids32 {
			ids32[i] = rng.Uint32()
			ids64[i] = rng.Uint64()
		}

		// Build all data structures
		bitmap32 := roaring.NewBitmap()
		bitmap64 := roaring64.NewBitmap()
		m := make(map[uint64]struct{}, size)
		s := make([]uint64, size)
		copy(s, ids64)
		for i, id := range ids64 {
			bitmap32.Add(ids32[i])
			bitmap64.Add(id)
			m[id] = struct{}{}
		}

		// Generate lookup targets: 50% hits, 50% misses
		numLookups := 10000
		lookups32 := make([]uint32, numLookups)
		lookups64 := make([]uint64, numLookups)
		for i := 0; i < numLookups/2; i++ {
			lookups32[i] = ids32[rng.Intn(len(ids32))] // hits
			lookups64[i] = ids64[rng.Intn(len(ids64))]
		}
		for i := numLookups / 2; i < numLookups; i++ {
			lookups32[i] = rng.Uint32() // likely misses
			lookups64[i] = rng.Uint64()
		}
		rng.Shuffle(len(lookups32), func(i, j int) {
			lookups32[i], lookups32[j] = lookups32[j], lookups32[i]
			lookups64[i], lookups64[j] = lookups64[j], lookups64[i]
		})

		name := fmt.Sprintf("n=%d", size)

		b.Run("Roaring32Sparse/"+name, func(b *testing.B) {
			i := 0
			for b.Loop() {
				_ = bitmap32.Contains(lookups32[i%len(lookups32)])
				i++
			}
		})

		b.Run("Roaring64/"+name, func(b *testing.B) {
			i := 0
			for b.Loop() {
				_ = bitmap64.Contains(lookups64[i%len(lookups64)])
				i++
			}
		})

		b.Run("PlainMap64/"+name, func(b *testing.B) {
			i := 0
			for b.Loop() {
				_, _ = m[lookups64[i%len(lookups64)]]
				i++
			}
		})

		b.Run("Slice64/"+name, func(b *testing.B) {
			i := 0
			for b.Loop() {
				_ = sliceContains(s, lookups64[i%len(lookups64)])
				i++
			}
		})
	}
}

// BenchmarkIntegerLookupDense tests lookup with dense sequential IDs (roaring's ideal case).
func BenchmarkIntegerLookupDense(b *testing.B) {
	sizes := []int{100, 1000, 10_000, 100_000, 1_000_000}

	for _, size := range sizes {
		// Dense sequential IDs: 0, 1, 2, ..., size-1
		bitmap32 := roaring.NewBitmap()
		bitmap64 := roaring64.NewBitmap()
		m := make(map[uint64]struct{}, size)
		s := make([]uint64, size)
		for i := 0; i < size; i++ {
			bitmap32.Add(uint32(i))
			bitmap64.Add(uint64(i))
			m[uint64(i)] = struct{}{}
			s[i] = uint64(i)
		}

		// Generate lookup targets: 50% hits (in range), 50% misses (out of range)
		rng := rand.New(rand.NewSource(42))
		numLookups := 10000
		lookups32 := make([]uint32, numLookups)
		lookups64 := make([]uint64, numLookups)
		for i := 0; i < numLookups/2; i++ {
			v := uint32(rng.Intn(size)) // hits
			lookups32[i] = v
			lookups64[i] = uint64(v)
		}
		for i := numLookups / 2; i < numLookups; i++ {
			v := uint32(size + rng.Intn(size)) // misses (out of range)
			lookups32[i] = v
			lookups64[i] = uint64(v)
		}
		rng.Shuffle(len(lookups32), func(i, j int) {
			lookups32[i], lookups32[j] = lookups32[j], lookups32[i]
			lookups64[i], lookups64[j] = lookups64[j], lookups64[i]
		})

		name := fmt.Sprintf("n=%d", size)

		b.Run("Roaring32/"+name, func(b *testing.B) {
			i := 0
			for b.Loop() {
				_ = bitmap32.Contains(lookups32[i%len(lookups32)])
				i++
			}
		})

		b.Run("Roaring64/"+name, func(b *testing.B) {
			i := 0
			for b.Loop() {
				_ = bitmap64.Contains(lookups64[i%len(lookups64)])
				i++
			}
		})

		b.Run("PlainMap64/"+name, func(b *testing.B) {
			i := 0
			for b.Loop() {
				_, _ = m[lookups64[i%len(lookups64)]]
				i++
			}
		})

		// Only run slice for smaller sizes (it's O(n))
		if size <= 10_000 {
			b.Run("Slice64/"+name, func(b *testing.B) {
				i := 0
				for b.Loop() {
					_ = sliceContains(s, lookups64[i%len(lookups64)])
					i++
				}
			})
		}
	}
}

// BenchmarkIntegerLookupSmall focuses on very small set sizes where slice might win.
func BenchmarkIntegerLookupSmall(b *testing.B) {
	sizes := []int{5, 10, 20, 50}

	for _, size := range sizes {
		rng := rand.New(rand.NewSource(42))
		ids := make([]uint64, size)
		for i := range ids {
			ids[i] = rng.Uint64()
		}

		bitmap := roaring64.NewBitmap()
		m := make(map[uint64]struct{}, size)
		s := make([]uint64, size)
		copy(s, ids)
		for _, id := range ids {
			bitmap.Add(id)
			m[id] = struct{}{}
		}

		// 50% hits, 50% misses
		numLookups := 10000
		lookups := make([]uint64, numLookups)
		for i := 0; i < numLookups/2; i++ {
			lookups[i] = ids[rng.Intn(len(ids))]
		}
		for i := numLookups / 2; i < numLookups; i++ {
			lookups[i] = rng.Uint64()
		}
		rng.Shuffle(len(lookups), func(i, j int) {
			lookups[i], lookups[j] = lookups[j], lookups[i]
		})

		name := fmt.Sprintf("n=%d", size)

		b.Run("Roaring64/"+name, func(b *testing.B) {
			i := 0
			for b.Loop() {
				_ = bitmap.Contains(lookups[i%len(lookups)])
				i++
			}
		})

		b.Run("PlainMap64/"+name, func(b *testing.B) {
			i := 0
			for b.Loop() {
				_, _ = m[lookups[i%len(lookups)]]
				i++
			}
		})

		b.Run("Slice64/"+name, func(b *testing.B) {
			i := 0
			for b.Loop() {
				_ = sliceContains(s, lookups[i%len(lookups)])
				i++
			}
		})
	}
}

// ============================================================================
// Density Spectrum Benchmarks
// ============================================================================

// generatePartiallyDenseIDs creates IDs with controlled sparsity (simple bimodal version).
// NOTE: This is NOT realistic - it creates one dense block + random scatter.
// Use generateRealisticIDs for more realistic patterns.
func generatePartiallyDenseIDs(n int, holeRatio float64, seed int64) []uint32 {
	if holeRatio <= 0 {
		// Fully dense
		ids := make([]uint32, n)
		for i := range ids {
			ids[i] = uint32(i)
		}
		return ids
	}
	if holeRatio >= 1 {
		// Fully random/sparse
		rng := rand.New(rand.NewSource(seed))
		ids := make([]uint32, n)
		for i := range ids {
			ids[i] = rng.Uint32()
		}
		return ids
	}

	rng := rand.New(rand.NewSource(seed))
	ids := make([]uint32, 0, n)

	numSequential := int(float64(n) * (1 - holeRatio))
	numRandom := n - numSequential

	// Add sequential IDs (dense portion)
	for i := 0; i < numSequential; i++ {
		ids = append(ids, uint32(i))
	}

	// Add random IDs (sparse portion) - scattered across uint32 space
	for i := 0; i < numRandom; i++ {
		ids = append(ids, rng.Uint32())
	}

	return ids
}

// generateRealisticIDs creates more realistic ID distributions.
// Pattern: multiple clusters of sequential IDs with gaps, plus some scattered IDs.
// - holeRatio controls the overall sparsity
// - Creates numClusters dense regions spread across the ID space
// - Each cluster has some internal holes (deleted IDs)
// - Some IDs are completely random (cross-cluster references)
func generateRealisticIDs(n int, holeRatio float64, seed int64) []uint32 {
	if holeRatio <= 0 {
		ids := make([]uint32, n)
		for i := range ids {
			ids[i] = uint32(i)
		}
		return ids
	}
	if holeRatio >= 1 {
		rng := rand.New(rand.NewSource(seed))
		ids := make([]uint32, n)
		for i := range ids {
			ids[i] = rng.Uint32()
		}
		return ids
	}

	rng := rand.New(rand.NewSource(seed))
	ids := make([]uint32, 0, n)

	// Create multiple clusters spread across the ID space
	numClusters := 10 + rng.Intn(20) // 10-30 clusters
	idsPerCluster := n / numClusters

	// Spread clusters across the uint32 space
	clusterSpacing := uint32(0xFFFFFFFF) / uint32(numClusters+1)

	for c := 0; c < numClusters && len(ids) < n; c++ {
		clusterStart := uint32(c+1) * clusterSpacing

		// Each cluster is mostly sequential but with some holes
		clusterSize := idsPerCluster + rng.Intn(idsPerCluster/2) - idsPerCluster/4
		if clusterSize <= 0 {
			clusterSize = idsPerCluster
		}

		// Internal hole probability based on holeRatio
		internalHoleProb := holeRatio * 0.5 // Half the sparsity comes from internal holes

		for i := 0; i < clusterSize && len(ids) < n; i++ {
			// Skip some IDs within the cluster (internal holes)
			if rng.Float64() < internalHoleProb {
				continue
			}
			ids = append(ids, clusterStart+uint32(i))
		}
	}

	// Add some completely random IDs (the other half of sparsity)
	numRandom := int(float64(n) * holeRatio * 0.5)
	for i := 0; i < numRandom && len(ids) < n; i++ {
		ids = append(ids, rng.Uint32())
	}

	// Fill remaining with more cluster IDs if needed
	lastClusterStart := uint32(numClusters) * clusterSpacing
	for len(ids) < n {
		ids = append(ids, lastClusterStart+uint32(len(ids)))
	}

	return ids
}

// generateRunsWithHoles creates runs of sequential IDs with random holes punched in.
// This simulates data with deletions - originally dense but some IDs removed.
func generateRunsWithHoles(n int, deletionRate float64, seed int64) []uint32 {
	rng := rand.New(rand.NewSource(seed))
	ids := make([]uint32, 0, n)

	// Generate more IDs than needed, then filter by deletion rate
	totalNeeded := int(float64(n) / (1 - deletionRate))
	if totalNeeded > 0x7FFFFFFF {
		totalNeeded = 0x7FFFFFFF
	}

	for i := 0; i < totalNeeded && len(ids) < n; i++ {
		if rng.Float64() >= deletionRate {
			ids = append(ids, uint32(i))
		}
	}

	return ids
}

// BenchmarkDensitySpectrum tests roaring performance across the density spectrum.
func BenchmarkDensitySpectrum(b *testing.B) {
	size := 100_000
	holeRatios := []float64{0.0, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99, 1.0}

	for _, holeRatio := range holeRatios {
		ids := generatePartiallyDenseIDs(size, holeRatio, 42)

		name := fmt.Sprintf("holes=%.0f%%", holeRatio*100)

		b.Run("Memory/Roaring32/"+name, func(b *testing.B) {
			for b.Loop() {
				before := measureMemory()
				bitmap := roaring.NewBitmap()
				for _, id := range ids {
					bitmap.Add(id)
				}
				bitmap.RunOptimize()
				after := measureMemory()

				b.ReportMetric(float64(after-before), "heap-bytes")
				b.ReportMetric(float64(after-before)/float64(size), "bytes/id")

				// Also report container breakdown
				stats := bitmap.Stats()
				b.ReportMetric(float64(stats.Containers), "containers")
				b.ReportMetric(float64(stats.RunContainers), "run-containers")
				b.ReportMetric(float64(stats.ArrayContainers), "array-containers")
				b.ReportMetric(float64(stats.BitmapContainers), "bitmap-containers")

				if bitmap.GetCardinality() == 0 {
					b.Fatal("unexpected empty bitmap")
				}
			}
		})

		b.Run("Memory/PlainMap32/"+name, func(b *testing.B) {
			for b.Loop() {
				before := measureMemory()
				m := make(map[uint32]struct{}, size)
				for _, id := range ids {
					m[id] = struct{}{}
				}
				after := measureMemory()

				b.ReportMetric(float64(after-before), "heap-bytes")
				b.ReportMetric(float64(after-before)/float64(size), "bytes/id")

				if len(m) == 0 {
					b.Fatal("unexpected empty map")
				}
			}
		})
	}

	// Now test lookup performance at each density level
	for _, holeRatio := range holeRatios {
		ids := generatePartiallyDenseIDs(size, holeRatio, 42)

		bitmap := roaring.NewBitmap()
		m := make(map[uint32]struct{}, size)
		for _, id := range ids {
			bitmap.Add(id)
			m[id] = struct{}{}
		}
		bitmap.RunOptimize()

		// Generate lookups: 50% hits, 50% misses
		rng := rand.New(rand.NewSource(123))
		numLookups := 10000
		lookups := make([]uint32, numLookups)
		for i := 0; i < numLookups/2; i++ {
			lookups[i] = ids[rng.Intn(len(ids))]
		}
		for i := numLookups / 2; i < numLookups; i++ {
			lookups[i] = rng.Uint32()
		}
		rng.Shuffle(len(lookups), func(i, j int) {
			lookups[i], lookups[j] = lookups[j], lookups[i]
		})

		b.Run(fmt.Sprintf("Lookup/Roaring32/holes=%.0f%%", holeRatio*100), func(b *testing.B) {
			i := 0
			for b.Loop() {
				_ = bitmap.Contains(lookups[i%len(lookups)])
				i++
			}
		})

		b.Run(fmt.Sprintf("Lookup/PlainMap32/holes=%.0f%%", holeRatio*100), func(b *testing.B) {
			i := 0
			for b.Loop() {
				_, _ = m[lookups[i%len(lookups)]]
				i++
			}
		})
	}
}

// BenchmarkRealisticPatterns compares different realistic ID patterns.
func BenchmarkRealisticPatterns(b *testing.B) {
	size := 100_000
	holeRatios := []float64{0.1, 0.25, 0.5}

	for _, holeRatio := range holeRatios {
		name := fmt.Sprintf("holes=%.0f%%", holeRatio*100)

		// Pattern 1: Bimodal (original - one dense block + random)
		bimodalIDs := generatePartiallyDenseIDs(size, holeRatio, 42)

		// Pattern 2: Realistic clusters
		clusterIDs := generateRealisticIDs(size, holeRatio, 42)

		// Pattern 3: Runs with deletions
		deletionIDs := generateRunsWithHoles(size, holeRatio, 42)

		patterns := []struct {
			name string
			ids  []uint32
		}{
			{"Bimodal", bimodalIDs},
			{"Clusters", clusterIDs},
			{"Deletions", deletionIDs},
		}

		for _, p := range patterns {
			b.Run(fmt.Sprintf("Memory/%s/%s", p.name, name), func(b *testing.B) {
				for b.Loop() {
					before := measureMemory()
					bitmap := roaring.NewBitmap()
					for _, id := range p.ids {
						bitmap.Add(id)
					}
					bitmap.RunOptimize()
					after := measureMemory()

					stats := bitmap.Stats()
					b.ReportMetric(float64(after-before)/float64(size), "bytes/id")
					b.ReportMetric(float64(stats.Containers), "containers")
					runPct := float64(stats.RunContainerValues) / float64(stats.Cardinality) * 100
					b.ReportMetric(runPct, "run%")

					if bitmap.GetCardinality() == 0 {
						b.Fatal("unexpected empty bitmap")
					}
				}
			})
		}
	}
}

// BenchmarkRealisticClustersRoaring64 tests roaring64 with realistic cluster patterns.
func BenchmarkRealisticClustersRoaring64(b *testing.B) {
	size := 100_000
	holeRatios := []float64{0.0, 0.1, 0.25, 0.5, 0.75, 1.0}

	for _, holeRatio := range holeRatios {
		// Generate 32-bit cluster IDs, then expand to 64-bit with different high bits
		clusterIDs32 := generateRealisticIDs(size, holeRatio, 42)

		name := fmt.Sprintf("holes=%.0f%%", holeRatio*100)

		// Test 1: 64-bit IDs with same high 32 bits (like a single shard)
		b.Run("Roaring64/SingleShard/"+name, func(b *testing.B) {
			for b.Loop() {
				before := measureMemory()
				bitmap := roaring64.NewBitmap()
				highBits := uint64(0x12345678) << 32
				for _, id := range clusterIDs32 {
					bitmap.Add(highBits | uint64(id))
				}
				after := measureMemory()

				b.ReportMetric(float64(after-before)/float64(size), "bytes/id")

				if bitmap.GetCardinality() == 0 {
					b.Fatal("unexpected empty bitmap")
				}
			}
		})

		// Test 2: 64-bit IDs spread across multiple shards (high bits vary)
		b.Run("Roaring64/MultiShard/"+name, func(b *testing.B) {
			rng := rand.New(rand.NewSource(42))
			numShards := 100

			for b.Loop() {
				before := measureMemory()
				bitmap := roaring64.NewBitmap()
				for _, id := range clusterIDs32 {
					// Assign each ID to a random shard
					shard := uint64(rng.Intn(numShards))
					bitmap.Add((shard << 32) | uint64(id))
				}
				after := measureMemory()

				b.ReportMetric(float64(after-before)/float64(size), "bytes/id")

				if bitmap.GetCardinality() == 0 {
					b.Fatal("unexpected empty bitmap")
				}
			}
		})

		// Test 3: Compare with roaring32
		b.Run("Roaring32/"+name, func(b *testing.B) {
			for b.Loop() {
				before := measureMemory()
				bitmap := roaring.NewBitmap()
				for _, id := range clusterIDs32 {
					bitmap.Add(id)
				}
				bitmap.RunOptimize()
				after := measureMemory()

				b.ReportMetric(float64(after-before)/float64(size), "bytes/id")

				if bitmap.GetCardinality() == 0 {
					b.Fatal("unexpected empty bitmap")
				}
			}
		})
	}
}

// BenchmarkRoaring64VsMap64 does a fair apples-to-apples comparison of roaring64 vs map[uint64]
// using the same 64-bit IDs with shards in high bits.
func BenchmarkRoaring64VsMap64(b *testing.B) {
	size := 100_000
	numShards := 100
	holeRatios := []float64{0.0, 0.1, 0.25, 0.5, 0.75, 1.0}

	for _, holeRatio := range holeRatios {
		// Generate realistic 32-bit cluster IDs, then expand to 64-bit with shard in high bits
		clusterIDs32 := generateRealisticIDs(size, holeRatio, 42)

		// Create 64-bit IDs: shard in high 32 bits, cluster ID in low 32 bits
		rng := rand.New(rand.NewSource(42))
		ids64 := make([]uint64, size)
		for i, id32 := range clusterIDs32 {
			shard := uint64(rng.Intn(numShards))
			ids64[i] = (shard << 32) | uint64(id32)
		}

		name := fmt.Sprintf("holes=%.0f%%", holeRatio*100)

		// Memory benchmarks
		b.Run("Memory/Roaring64/"+name, func(b *testing.B) {
			for b.Loop() {
				before := measureMemory()
				bitmap := roaring64.NewBitmap()
				for _, id := range ids64 {
					bitmap.Add(id)
				}
				after := measureMemory()

				b.ReportMetric(float64(after-before)/float64(size), "bytes/id")

				if bitmap.GetCardinality() == 0 {
					b.Fatal("unexpected empty bitmap")
				}
			}
		})

		b.Run("Memory/PlainMap64/"+name, func(b *testing.B) {
			for b.Loop() {
				before := measureMemory()
				m := make(map[uint64]struct{}, size)
				for _, id := range ids64 {
					m[id] = struct{}{}
				}
				after := measureMemory()

				b.ReportMetric(float64(after-before)/float64(size), "bytes/id")

				if len(m) == 0 {
					b.Fatal("unexpected empty map")
				}
			}
		})

		b.Run("Memory/Slice64/"+name, func(b *testing.B) {
			for b.Loop() {
				sliceSink = nil
				before := measureMemory()
				s := make([]uint64, size)
				copy(s, ids64)
				sliceSink = s
				after := measureMemory()

				b.ReportMetric(float64(after-before)/float64(size), "bytes/id")

				if len(s) == 0 {
					b.Fatal("unexpected empty slice")
				}
			}
		})
	}

	// Lookup benchmarks with same 64-bit IDs
	for _, holeRatio := range holeRatios {
		clusterIDs32 := generateRealisticIDs(size, holeRatio, 42)
		rng := rand.New(rand.NewSource(42))
		ids64 := make([]uint64, size)
		for i, id32 := range clusterIDs32 {
			shard := uint64(rng.Intn(numShards))
			ids64[i] = (shard << 32) | uint64(id32)
		}

		// Build data structures
		bitmap := roaring64.NewBitmap()
		m := make(map[uint64]struct{}, size)
		for _, id := range ids64 {
			bitmap.Add(id)
			m[id] = struct{}{}
		}

		// Generate lookups: 50% hits, 50% misses
		rng = rand.New(rand.NewSource(123))
		numLookups := 10000
		lookups := make([]uint64, numLookups)
		for i := 0; i < numLookups/2; i++ {
			lookups[i] = ids64[rng.Intn(len(ids64))]
		}
		for i := numLookups / 2; i < numLookups; i++ {
			lookups[i] = rng.Uint64()
		}
		rng.Shuffle(len(lookups), func(i, j int) {
			lookups[i], lookups[j] = lookups[j], lookups[i]
		})

		name := fmt.Sprintf("holes=%.0f%%", holeRatio*100)

		b.Run("Lookup/Roaring64/"+name, func(b *testing.B) {
			i := 0
			for b.Loop() {
				_ = bitmap.Contains(lookups[i%len(lookups)])
				i++
			}
		})

		b.Run("Lookup/PlainMap64/"+name, func(b *testing.B) {
			i := 0
			for b.Loop() {
				_, _ = m[lookups[i%len(lookups)]]
				i++
			}
		})
	}
}

// BenchmarkDensitySpectrumDetailed prints detailed stats for each density level.
func BenchmarkDensitySpectrumDetailed(b *testing.B) {
	size := 1_000_000
	holeRatios := []float64{0.0, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0}

	for _, holeRatio := range holeRatios {
		ids := generatePartiallyDenseIDs(size, holeRatio, 42)

		name := fmt.Sprintf("holes=%.0f%%", holeRatio*100)

		b.Run(name, func(b *testing.B) {
			for b.Loop() {
				before := measureMemory()
				bitmap := roaring.NewBitmap()
				for _, id := range ids {
					bitmap.Add(id)
				}
				bitmap.RunOptimize()
				after := measureMemory()

				stats := bitmap.Stats()
				memBytes := after - before
				bytesPerID := float64(memBytes) / float64(size)

				b.ReportMetric(bytesPerID, "bytes/id")
				b.ReportMetric(float64(stats.Containers), "containers")

				// Calculate percentage of values in each container type
				runPct := float64(stats.RunContainerValues) / float64(stats.Cardinality) * 100
				arrayPct := float64(stats.ArrayContainerValues) / float64(stats.Cardinality) * 100
				bitmapPct := float64(stats.BitmapContainerValues) / float64(stats.Cardinality) * 100

				b.ReportMetric(runPct, "run%")
				b.ReportMetric(arrayPct, "array%")
				b.ReportMetric(bitmapPct, "bitmap%")

				if bitmap.GetCardinality() == 0 {
					b.Fatal("unexpected empty bitmap")
				}
			}
		})
	}
}

// BenchmarkMultiSetVaryMembership tests different membership densities.
// Sparse membership = more compression opportunity for roaring.
func BenchmarkMultiSetVaryMembership(b *testing.B) {
	membershipPcts := []float64{0.01, 0.05, 0.1, 0.25, 0.5}
	numSets := 100
	numIDs := 10_000

	for _, pct := range membershipPcts {
		cfg := multiSetConfig{numSets: numSets, numIDs: numIDs, membershipPct: pct}
		roaringSets, plainSets, allIDs, _ := buildMultiSetData(cfg, 42)

		// All lookups are hits (from the ID universe)
		rng := rand.New(rand.NewSource(123))
		lookupIDs := make([]string, 1000)
		for i := range lookupIDs {
			lookupIDs[i] = allIDs[rng.Intn(len(allIDs))]
		}

		name := fmt.Sprintf("mem=%.0f%%", pct*100)

		b.Run("SharedRoaring/"+name, func(b *testing.B) {
			for b.Loop() {
				for _, id := range lookupIDs {
					_ = findFirstContainingRoaring(roaringSets, id)
				}
			}
		})

		b.Run("PlainSet/"+name, func(b *testing.B) {
			for b.Loop() {
				for _, id := range lookupIDs {
					_ = findFirstContainingPlain(plainSets, id)
				}
			}
		})
	}
}
