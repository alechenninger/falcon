package falcon

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"runtime"
	"testing"

	"github.com/RoaringBitmap/roaring"
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
		rng := rand.New(rand.NewSource(42))
		bitmap := roaring.NewBitmap()
		for i := 0; i < size; i++ {
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
