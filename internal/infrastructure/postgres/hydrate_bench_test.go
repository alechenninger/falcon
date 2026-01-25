package postgres_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/alechenninger/falcon/internal/domain"
	infrapostgres "github.com/alechenninger/falcon/internal/infrastructure/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// HydrationScaleConfig defines parameters for generating test tuples.
// Similar to domain.ScaleConfig but focused on tuple generation for DB insertion.
type HydrationScaleConfig struct {
	Name string

	// Hierarchy parameters
	FolderDepth          int
	FoldersPerLevel      int
	DocumentsPerFolder   int
	ParentSpreadPerLevel int

	// Group parameters
	NumGroups              int
	UsersPerGroup          int
	GroupsPerFolder        int
	DirectViewersPerFolder int
	NumUsers               int
}

// TupleCount estimates the total number of tuples for this configuration.
func (c HydrationScaleConfig) TupleCount() int64 {
	numLeafFolders := c.FoldersPerLevel
	for i := 1; i < c.FolderDepth; i++ {
		numLeafFolders *= c.FoldersPerLevel
	}
	if c.FolderDepth == 0 {
		numLeafFolders = c.FoldersPerLevel
	}

	totalFolders := 0
	foldersAtLevel := c.FoldersPerLevel
	for i := 0; i <= c.FolderDepth; i++ {
		totalFolders += foldersAtLevel
		foldersAtLevel *= c.FoldersPerLevel
	}

	nonRootFolders := totalFolders - c.FoldersPerLevel
	parentTuples := int64(nonRootFolders) * int64(c.ParentSpreadPerLevel)
	folderViewerTuples := int64(totalFolders) * int64(c.GroupsPerFolder+c.DirectViewersPerFolder)
	documentTuples := int64(numLeafFolders) * int64(c.DocumentsPerFolder)
	groupMemberTuples := int64(c.NumGroups) * int64(c.UsersPerGroup)

	return parentTuples + folderViewerTuples + documentTuples + groupMemberTuples
}

// hydrationScaleConfigs defines the scale points for benchmarking.
// Subset of domain.scaleConfigs focused on reasonable benchmark times.
var hydrationScaleConfigs = []HydrationScaleConfig{
	{
		Name:                   "Small/20K",
		FolderDepth:            2,
		FoldersPerLevel:        5,
		DocumentsPerFolder:     100,
		ParentSpreadPerLevel:   1,
		NumGroups:              50,
		UsersPerGroup:          100,
		GroupsPerFolder:        3,
		DirectViewersPerFolder: 5,
		NumUsers:               5_000,
	},
	{
		Name:                   "Medium/250K",
		FolderDepth:            3,
		FoldersPerLevel:        5,
		DocumentsPerFolder:     200,
		ParentSpreadPerLevel:   1,
		NumGroups:              200,
		UsersPerGroup:          500,
		GroupsPerFolder:        5,
		DirectViewersPerFolder: 10,
		NumUsers:               20_000,
	},
	{
		Name:                   "Large/2.7M",
		FolderDepth:            4,
		FoldersPerLevel:        5,
		DocumentsPerFolder:     500,
		ParentSpreadPerLevel:   1,
		NumGroups:              1_000,
		UsersPerGroup:          1_000,
		GroupsPerFolder:        10,
		DirectViewersPerFolder: 20,
		NumUsers:               100_000,
	},
}

// benchSchema creates a schema for hydration benchmarking.
// Type IDs: user=1, group=2, folder=3, document=4
// Relation IDs: member=1, parent=1, viewer=2, editor=3
func benchSchema() *domain.Schema {
	s := &domain.Schema{
		Types: map[domain.TypeName]*domain.ObjectType{
			"user": {
				ID:        1,
				Name:      "user",
				Relations: map[domain.RelationName]*domain.Relation{},
			},
			"group": {
				ID:   2,
				Name: "group",
				Relations: map[domain.RelationName]*domain.Relation{
					"member": {
						ID:   1,
						Name: "member",
						Usersets: []domain.Userset{
							domain.Direct(domain.Ref("user")),
						},
					},
				},
			},
			"folder": {
				ID:   3,
				Name: "folder",
				Relations: map[domain.RelationName]*domain.Relation{
					"parent": {
						ID:   1,
						Name: "parent",
						Usersets: []domain.Userset{
							domain.Direct(domain.Ref("folder")),
						},
					},
					"viewer": {
						ID:   2,
						Name: "viewer",
						Usersets: []domain.Userset{
							domain.Direct(
								domain.Ref("user"),
								domain.RefWithRelation("group", "member"),
							),
							domain.Arrow("parent", "viewer"),
						},
					},
					"editor": {
						ID:   3,
						Name: "editor",
						Usersets: []domain.Userset{
							domain.Direct(
								domain.Ref("user"),
								domain.RefWithRelation("group", "member"),
							),
							domain.Arrow("parent", "editor"),
						},
					},
				},
			},
			"document": {
				ID:   4,
				Name: "document",
				Relations: map[domain.RelationName]*domain.Relation{
					"parent": {
						ID:   1,
						Name: "parent",
						Usersets: []domain.Userset{
							domain.Direct(domain.Ref("folder")),
						},
					},
					"viewer": {
						ID:   2,
						Name: "viewer",
						Usersets: []domain.Userset{
							domain.Direct(
								domain.Ref("user"),
								domain.RefWithRelation("group", "member"),
							),
							domain.Arrow("parent", "viewer"),
						},
					},
					"editor": {
						ID:   3,
						Name: "editor",
						Usersets: []domain.Userset{
							domain.Direct(
								domain.Ref("user"),
								domain.RefWithRelation("group", "member"),
							),
							domain.Arrow("parent", "editor"),
						},
					},
				},
			},
		},
	}
	s.Compile()
	return s
}

// generateTuples creates tuples according to the scale configuration.
// This mirrors the logic in domain.buildLargeGraph but returns raw tuples.
func generateTuples(cfg HydrationScaleConfig, s *domain.Schema) []domain.Tuple {
	rng := rand.New(rand.NewSource(42))
	tuples := make([]domain.Tuple, 0, cfg.TupleCount())

	// Track folder IDs at each level
	foldersByLevel := make([][]domain.ID, cfg.FolderDepth+1)
	nextFolderID := domain.ID(1)

	// Create root folders
	foldersByLevel[0] = make([]domain.ID, cfg.FoldersPerLevel)
	for i := 0; i < cfg.FoldersPerLevel; i++ {
		foldersByLevel[0][i] = nextFolderID
		nextFolderID++
	}

	// Build folder hierarchy
	for level := 1; level <= cfg.FolderDepth; level++ {
		parentFolders := foldersByLevel[level-1]
		childFolders := make([]domain.ID, 0, len(parentFolders)*cfg.FoldersPerLevel)

		for _, parentID := range parentFolders {
			for i := 0; i < cfg.FoldersPerLevel; i++ {
				childID := nextFolderID
				nextFolderID++
				childFolders = append(childFolders, childID)

				// Add parent relationship
				tuples = append(tuples, domain.Tuple{
					ObjectType:      s.GetTypeID("folder"),
					ObjectID:        childID,
					Relation:        s.GetRelationID("folder", "parent"),
					SubjectType:     s.GetTypeID("folder"),
					SubjectID:       parentID,
					SubjectRelation: domain.NoRelation,
				})

				// Additional parents for DAG structure
				if cfg.ParentSpreadPerLevel > 1 && len(parentFolders) > 1 {
					for j := 1; j < cfg.ParentSpreadPerLevel && j < len(parentFolders); j++ {
						altParent := parentFolders[rng.Intn(len(parentFolders))]
						if altParent != parentID {
							tuples = append(tuples, domain.Tuple{
								ObjectType:      s.GetTypeID("folder"),
								ObjectID:        childID,
								Relation:        s.GetRelationID("folder", "parent"),
								SubjectType:     s.GetTypeID("folder"),
								SubjectID:       altParent,
								SubjectRelation: domain.NoRelation,
							})
						}
					}
				}
			}
		}
		foldersByLevel[level] = childFolders
	}

	// Create groups and users
	for groupID := domain.ID(1); groupID <= domain.ID(cfg.NumGroups); groupID++ {
		for i := 0; i < cfg.UsersPerGroup; i++ {
			userID := domain.ID(rng.Intn(cfg.NumUsers) + 1)
			tuples = append(tuples, domain.Tuple{
				ObjectType:      s.GetTypeID("group"),
				ObjectID:        groupID,
				Relation:        s.GetRelationID("group", "member"),
				SubjectType:     s.GetTypeID("user"),
				SubjectID:       userID,
				SubjectRelation: domain.NoRelation,
			})
		}
	}

	// Collect all folders
	allFolders := make([]domain.ID, 0)
	for _, level := range foldersByLevel {
		allFolders = append(allFolders, level...)
	}

	// Add viewers to folders
	for _, folderID := range allFolders {
		// Group viewers
		for i := 0; i < cfg.GroupsPerFolder; i++ {
			groupID := domain.ID(rng.Intn(cfg.NumGroups) + 1)
			tuples = append(tuples, domain.Tuple{
				ObjectType:      s.GetTypeID("folder"),
				ObjectID:        folderID,
				Relation:        s.GetRelationID("folder", "viewer"),
				SubjectType:     s.GetTypeID("group"),
				SubjectID:       groupID,
				SubjectRelation: s.GetRelationID("group", "member"),
			})
		}

		// Direct viewers
		for i := 0; i < cfg.DirectViewersPerFolder; i++ {
			userID := domain.ID(rng.Intn(cfg.NumUsers) + 1)
			tuples = append(tuples, domain.Tuple{
				ObjectType:      s.GetTypeID("folder"),
				ObjectID:        folderID,
				Relation:        s.GetRelationID("folder", "viewer"),
				SubjectType:     s.GetTypeID("user"),
				SubjectID:       userID,
				SubjectRelation: domain.NoRelation,
			})
		}
	}

	// Create documents in leaf folders
	leafFolders := foldersByLevel[cfg.FolderDepth]
	nextDocID := domain.ID(1)
	for _, folderID := range leafFolders {
		for i := 0; i < cfg.DocumentsPerFolder; i++ {
			tuples = append(tuples, domain.Tuple{
				ObjectType:      s.GetTypeID("document"),
				ObjectID:        nextDocID,
				Relation:        s.GetRelationID("document", "parent"),
				SubjectType:     s.GetTypeID("folder"),
				SubjectID:       folderID,
				SubjectRelation: domain.NoRelation,
			})
			nextDocID++
		}
	}

	return tuples
}

// bulkInsertTuples inserts tuples into PostgreSQL using batch operations.
func bulkInsertTuples(ctx context.Context, pool *pgxpool.Pool, tuples []domain.Tuple) error {
	const batchSize = 10000

	for i := 0; i < len(tuples); i += batchSize {
		end := i + batchSize
		if end > len(tuples) {
			end = len(tuples)
		}
		batch := tuples[i:end]

		// Build batch insert
		query := "INSERT INTO tuples (object_type, object_id, relation, subject_type, subject_id, subject_relation) VALUES "
		args := make([]any, 0, len(batch)*6)
		for j, t := range batch {
			if j > 0 {
				query += ", "
			}
			argOffset := j * 6
			query += fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)",
				argOffset+1, argOffset+2, argOffset+3, argOffset+4, argOffset+5, argOffset+6)
			args = append(args,
				int16(t.ObjectType), int64(t.ObjectID), int16(t.Relation),
				int16(t.SubjectType), int64(t.SubjectID), int16(t.SubjectRelation))
		}
		query += " ON CONFLICT DO NOTHING"

		_, err := pool.Exec(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("batch insert failed at offset %d: %w", i, err)
		}
	}

	return nil
}

// measureHeap forces GC and returns heap bytes.
func measureHeap() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// setupPostgres starts a PostgreSQL container and returns the connection string.
// Returns a cleanup function that should be called when done.
func setupPostgres(ctx context.Context, t testing.TB) (string, func()) {
	// Disable Ryuk for podman compatibility
	os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	pgContainer, err := postgres.Run(ctx,
		"postgres:18-alpine",
		postgres.WithDatabase("falcon_bench"),
		postgres.WithUsername("bench"),
		postgres.WithPassword("bench"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		pgContainer.Terminate(ctx)
		t.Fatalf("failed to get connection string: %v", err)
	}

	cleanup := func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate postgres container: %v", err)
		}
	}

	return connStr, cleanup
}

// BenchmarkHydration benchmarks the full hydration process from PostgreSQL.
func BenchmarkHydration(b *testing.B) {
	ctx := context.Background()
	connStr, cleanup := setupPostgres(ctx, b)
	defer cleanup()

	// Create store and schema
	store, err := infrapostgres.NewStore(ctx, connStr)
	if err != nil {
		b.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	if err := store.EnsureSchema(ctx); err != nil {
		b.Fatalf("failed to create schema: %v", err)
	}

	// Get pool for bulk insert (need to access the pool directly)
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		b.Fatalf("failed to create pool: %v", err)
	}
	defer pool.Close()

	schema := benchSchema()

	for _, cfg := range hydrationScaleConfigs {
		b.Run(cfg.Name, func(b *testing.B) {
			// Generate tuples
			b.Logf("Generating ~%d tuples...", cfg.TupleCount())
			tuples := generateTuples(cfg, schema)
			b.Logf("Generated %d tuples", len(tuples))

			// Truncate and populate
			_, err := pool.Exec(ctx, "TRUNCATE tuples")
			if err != nil {
				b.Fatalf("failed to truncate: %v", err)
			}

			b.Logf("Inserting tuples into PostgreSQL...")
			insertStart := time.Now()
			if err := bulkInsertTuples(ctx, pool, tuples); err != nil {
				b.Fatalf("failed to insert tuples: %v", err)
			}
			b.Logf("Inserted %d tuples in %v", len(tuples), time.Since(insertStart))

			// Benchmark: DB-only (iterate without building structures)
			b.Run("DB-only", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					iter, err := store.LoadAll(ctx)
					if err != nil {
						b.Fatalf("LoadAll failed: %v", err)
					}
					count := 0
					for iter.Next() {
						_ = iter.Tuple()
						count++
					}
					if err := iter.Err(); err != nil {
						b.Fatalf("iteration error: %v", err)
					}
					iter.Close()
				}
				b.ReportMetric(float64(len(tuples)), "tuples")
				b.ReportMetric(float64(len(tuples))*float64(b.N)/b.Elapsed().Seconds(), "tuples/sec")
			})

			// Benchmark: Hydrate-only (from pre-loaded slice)
			b.Run("Hydrate-only", func(b *testing.B) {
				// Pre-load tuples into a slice iterator
				sliceIter := domain.NewSliceIterator(tuples)

				beforeMem := measureHeap()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					usersets := domain.NewMultiversionUsersets(schema)
					// Reset iterator for each run
					sliceIter = domain.NewSliceIterator(tuples)
					if err := usersets.Hydrate(sliceIter); err != nil {
						b.Fatalf("Hydrate failed: %v", err)
					}
				}
				b.StopTimer()
				afterMem := measureHeap()

				memUsed := afterMem - beforeMem
				bytesPerTuple := float64(memUsed) / float64(len(tuples))
				b.ReportMetric(float64(len(tuples)), "tuples")
				b.ReportMetric(float64(len(tuples))*float64(b.N)/b.Elapsed().Seconds(), "tuples/sec")
				b.ReportMetric(bytesPerTuple, "bytes/tuple")
			})

			// Benchmark: End-to-end (LoadAll + Hydrate)
			b.Run("End-to-end", func(b *testing.B) {
				beforeMem := measureHeap()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					usersets := domain.NewMultiversionUsersets(schema)
					iter, err := store.LoadAll(ctx)
					if err != nil {
						b.Fatalf("LoadAll failed: %v", err)
					}
					if err := usersets.Hydrate(iter); err != nil {
						iter.Close()
						b.Fatalf("Hydrate failed: %v", err)
					}
					iter.Close()
				}
				b.StopTimer()
				afterMem := measureHeap()

				memUsed := afterMem - beforeMem
				bytesPerTuple := float64(memUsed) / float64(len(tuples))
				b.ReportMetric(float64(len(tuples)), "tuples")
				b.ReportMetric(float64(len(tuples))*float64(b.N)/b.Elapsed().Seconds(), "tuples/sec")
				b.ReportMetric(bytesPerTuple, "bytes/tuple")
			})
		})
	}
}

// BenchmarkHydrationOptimizations compares different optimization strategies.
func BenchmarkHydrationOptimizations(b *testing.B) {
	ctx := context.Background()
	connStr, cleanup := setupPostgres(ctx, b)
	defer cleanup()

	store, err := infrapostgres.NewStore(ctx, connStr)
	if err != nil {
		b.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	if err := store.EnsureSchema(ctx); err != nil {
		b.Fatalf("failed to create schema: %v", err)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		b.Fatalf("failed to create pool: %v", err)
	}
	defer pool.Close()

	schema := benchSchema()

	// Use Medium config for optimization comparison
	cfg := hydrationScaleConfigs[1] // Medium/250K

	b.Logf("Generating ~%d tuples...", cfg.TupleCount())
	tuples := generateTuples(cfg, schema)
	b.Logf("Generated %d tuples", len(tuples))

	_, err = pool.Exec(ctx, "TRUNCATE tuples")
	if err != nil {
		b.Fatalf("failed to truncate: %v", err)
	}

	if err := bulkInsertTuples(ctx, pool, tuples); err != nil {
		b.Fatalf("failed to insert tuples: %v", err)
	}

	tupleCount := int64(len(tuples))

	// Baseline: Iterator-based loading + standard Hydrate
	b.Run("Baseline", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			usersets := domain.NewMultiversionUsersets(schema)
			iter, err := store.LoadAll(ctx)
			if err != nil {
				b.Fatalf("LoadAll failed: %v", err)
			}
			if err := usersets.Hydrate(iter); err != nil {
				iter.Close()
				b.Fatalf("Hydrate failed: %v", err)
			}
			iter.Close()
		}
		b.ReportMetric(float64(tupleCount), "tuples")
		b.ReportMetric(float64(tupleCount)*float64(b.N)/b.Elapsed().Seconds(), "tuples/sec")
	})

	// Optimization 1: Pre-sized slice loading (avoids slice growth)
	b.Run("PreallocSlice", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			usersets := domain.NewMultiversionUsersets(schema)
			loadedTuples, err := store.LoadAllBatched(ctx, tupleCount)
			if err != nil {
				b.Fatalf("LoadAllBatched failed: %v", err)
			}
			sliceIter := domain.NewSliceIterator(loadedTuples)
			if err := usersets.Hydrate(sliceIter); err != nil {
				b.Fatalf("Hydrate failed: %v", err)
			}
		}
		b.ReportMetric(float64(tupleCount), "tuples")
		b.ReportMetric(float64(tupleCount)*float64(b.N)/b.Elapsed().Seconds(), "tuples/sec")
	})

	// Optimization 2: Pre-sized slice + HydrateSlice (avoids iterator overhead)
	b.Run("PreallocSlice+HydrateSlice", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			usersets := domain.NewMultiversionUsersets(schema)
			loadedTuples, err := store.LoadAllBatched(ctx, tupleCount)
			if err != nil {
				b.Fatalf("LoadAllBatched failed: %v", err)
			}
			usersets.HydrateFromSlice(loadedTuples)
		}
		b.ReportMetric(float64(tupleCount), "tuples")
		b.ReportMetric(float64(tupleCount)*float64(b.N)/b.Elapsed().Seconds(), "tuples/sec")
	})

	// Optimization 3: Pre-sized map + Pre-sized slice + HydrateSlice
	// Estimate map capacity as tupleCount / 20 (typical subjects per userset)
	mapCapacity := int(tupleCount / 20)
	b.Run("PreallocAll", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			usersets := domain.NewMultiversionUsersetsWithCapacity(schema, mapCapacity)
			loadedTuples, err := store.LoadAllBatched(ctx, tupleCount)
			if err != nil {
				b.Fatalf("LoadAllBatched failed: %v", err)
			}
			usersets.HydrateFromSlice(loadedTuples)
		}
		b.ReportMetric(float64(tupleCount), "tuples")
		b.ReportMetric(float64(tupleCount)*float64(b.N)/b.Elapsed().Seconds(), "tuples/sec")
	})

	// Optimization 4: COUNT query + all pre-allocations
	b.Run("CountQuery+PreallocAll", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// First, get count (simulates real-world where we don't know the count)
			count, err := store.TupleCount(ctx)
			if err != nil {
				b.Fatalf("TupleCount failed: %v", err)
			}
			usersets := domain.NewMultiversionUsersetsWithCapacity(schema, int(count/20))
			loadedTuples, err := store.LoadAllBatched(ctx, count)
			if err != nil {
				b.Fatalf("LoadAllBatched failed: %v", err)
			}
			usersets.HydrateFromSlice(loadedTuples)
		}
		b.ReportMetric(float64(tupleCount), "tuples")
		b.ReportMetric(float64(tupleCount)*float64(b.N)/b.Elapsed().Seconds(), "tuples/sec")
	})

	// Optimization 5: Pipelined - overlaps DB read with hydration via channel
	// This might help if CPU is idle while waiting for DB I/O
	b.Run("Pipelined", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			usersets := domain.NewMultiversionUsersets(schema)

			iter, err := store.LoadAll(ctx)
			if err != nil {
				b.Fatalf("LoadAll failed: %v", err)
			}

			// Channel to pipeline tuples from DB read to hydration
			tupleChan := make(chan domain.Tuple, 10000)
			errChan := make(chan error, 1)

			// Producer: read from DB
			go func() {
				defer close(tupleChan)
				for iter.Next() {
					tupleChan <- iter.Tuple()
				}
				if err := iter.Err(); err != nil {
					errChan <- err
				}
				iter.Close()
			}()

			// Consumer: hydrate (in main goroutine)
			usersets.HydrateFromChannel(tupleChan)

			// Check for errors
			select {
			case err := <-errChan:
				b.Fatalf("Iterator error: %v", err)
			default:
			}
		}
		b.ReportMetric(float64(tupleCount), "tuples")
		b.ReportMetric(float64(tupleCount)*float64(b.N)/b.Elapsed().Seconds(), "tuples/sec")
	})

	// Optimization 5b: Batched Pipeline - reduces channel overhead
	// Sends slices of tuples instead of individual tuples
	const batchSize = 10000
	b.Run("PipelinedBatched", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			usersets := domain.NewMultiversionUsersets(schema)

			iter, err := store.LoadAll(ctx)
			if err != nil {
				b.Fatalf("LoadAll failed: %v", err)
			}

			// Channel of batches - much less overhead than per-tuple
			batchChan := make(chan []domain.Tuple, 10)
			errChan := make(chan error, 1)

			// Producer: read from DB in batches
			go func() {
				defer close(batchChan)
				batch := make([]domain.Tuple, 0, batchSize)
				for iter.Next() {
					batch = append(batch, iter.Tuple())
					if len(batch) >= batchSize {
						batchChan <- batch
						batch = make([]domain.Tuple, 0, batchSize)
					}
				}
				if len(batch) > 0 {
					batchChan <- batch
				}
				if err := iter.Err(); err != nil {
					errChan <- err
				}
				iter.Close()
			}()

			// Consumer: hydrate batches
			usersets.HydrateFromBatchChannel(batchChan)

			select {
			case err := <-errChan:
				b.Fatalf("Iterator error: %v", err)
			default:
			}
		}
		b.ReportMetric(float64(tupleCount), "tuples")
		b.ReportMetric(float64(tupleCount)*float64(b.N)/b.Elapsed().Seconds(), "tuples/sec")
	})

	// Optimization 6: COPY protocol - uses PostgreSQL's bulk export
	// COPY has less protocol overhead than SELECT for bulk data
	b.Run("COPY", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			usersets := domain.NewMultiversionUsersets(schema)
			loadedTuples, err := store.LoadAllCopy(ctx, tupleCount)
			if err != nil {
				b.Fatalf("LoadAllCopy failed: %v", err)
			}
			usersets.HydrateFromSlice(loadedTuples)
		}
		b.ReportMetric(float64(tupleCount), "tuples")
		b.ReportMetric(float64(tupleCount)*float64(b.N)/b.Elapsed().Seconds(), "tuples/sec")
	})

	// Optimization 7: COPY + pre-sized map
	b.Run("COPY+PreallocMap", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			usersets := domain.NewMultiversionUsersetsWithCapacity(schema, mapCapacity)
			loadedTuples, err := store.LoadAllCopy(ctx, tupleCount)
			if err != nil {
				b.Fatalf("LoadAllCopy failed: %v", err)
			}
			usersets.HydrateFromSlice(loadedTuples)
		}
		b.ReportMetric(float64(tupleCount), "tuples")
		b.ReportMetric(float64(tupleCount)*float64(b.N)/b.Elapsed().Seconds(), "tuples/sec")
	})
}

// TestHydrationOptimizationsCorrectness verifies all optimization strategies
// produce identical results. This is critical for ensuring optimizations
// don't break correctness.
func TestHydrationOptimizationsCorrectness(t *testing.T) {
	ctx := context.Background()
	connStr, cleanup := setupPostgres(ctx, t)
	defer cleanup()

	store, err := infrapostgres.NewStore(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	defer pool.Close()

	schema := benchSchema()

	// Use Small config for correctness testing
	cfg := hydrationScaleConfigs[0]
	tuples := generateTuples(cfg, schema)

	_, err = pool.Exec(ctx, "TRUNCATE tuples")
	if err != nil {
		t.Fatalf("failed to truncate: %v", err)
	}

	if err := bulkInsertTuples(ctx, pool, tuples); err != nil {
		t.Fatalf("failed to insert tuples: %v", err)
	}

	tupleCount := int64(len(tuples))

	// Method 1: Baseline (iterator)
	usersets1 := domain.NewMultiversionUsersets(schema)
	iter, err := store.LoadAll(ctx)
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}
	if err := usersets1.Hydrate(iter); err != nil {
		iter.Close()
		t.Fatalf("Hydrate failed: %v", err)
	}
	iter.Close()

	// Method 2: Pre-sized slice + HydrateSlice
	usersets2 := domain.NewMultiversionUsersets(schema)
	loadedTuples, err := store.LoadAllBatched(ctx, tupleCount)
	if err != nil {
		t.Fatalf("LoadAllBatched failed: %v", err)
	}
	usersets2.HydrateFromSlice(loadedTuples)

	// Method 3: Pre-sized map + slice + HydrateSlice
	usersets3 := domain.NewMultiversionUsersetsWithCapacity(schema, int(tupleCount/20))
	loadedTuples2, err := store.LoadAllBatched(ctx, tupleCount)
	if err != nil {
		t.Fatalf("LoadAllBatched failed: %v", err)
	}
	usersets3.HydrateFromSlice(loadedTuples2)

	// Verify all methods produce same results by checking a sample of lookups
	// We'll check direct membership for several tuples
	for i := 0; i < 100 && i < len(tuples); i++ {
		tuple := tuples[i]
		key := domain.UsersetKey{
			ObjectType:      tuple.ObjectType,
			ObjectID:        tuple.ObjectID,
			Relation:        tuple.Relation,
			SubjectType:     tuple.SubjectType,
			SubjectRelation: tuple.SubjectRelation,
		}

		// Check using ContainsDirectWithin or ContainsUsersetSubjectWithin
		var found1, found2, found3 bool
		if tuple.SubjectRelation == domain.NoRelation {
			found1, _ = usersets1.ContainsDirectWithin(
				tuple.ObjectType, tuple.ObjectID, tuple.Relation,
				tuple.SubjectType, tuple.SubjectID,
				domain.MaxSnapshotWindow)
			found2, _ = usersets2.ContainsDirectWithin(
				tuple.ObjectType, tuple.ObjectID, tuple.Relation,
				tuple.SubjectType, tuple.SubjectID,
				domain.MaxSnapshotWindow)
			found3, _ = usersets3.ContainsDirectWithin(
				tuple.ObjectType, tuple.ObjectID, tuple.Relation,
				tuple.SubjectType, tuple.SubjectID,
				domain.MaxSnapshotWindow)
		} else {
			found1, _ = usersets1.ContainsUsersetSubjectWithin(
				tuple.ObjectType, tuple.ObjectID, tuple.Relation,
				tuple.SubjectType, tuple.SubjectID, tuple.SubjectRelation,
				domain.MaxSnapshotWindow)
			found2, _ = usersets2.ContainsUsersetSubjectWithin(
				tuple.ObjectType, tuple.ObjectID, tuple.Relation,
				tuple.SubjectType, tuple.SubjectID, tuple.SubjectRelation,
				domain.MaxSnapshotWindow)
			found3, _ = usersets3.ContainsUsersetSubjectWithin(
				tuple.ObjectType, tuple.ObjectID, tuple.Relation,
				tuple.SubjectType, tuple.SubjectID, tuple.SubjectRelation,
				domain.MaxSnapshotWindow)
		}

		if !found1 || !found2 || !found3 {
			t.Errorf("Tuple %d (key=%+v): baseline=%v, slice=%v, preallocAll=%v",
				i, key, found1, found2, found3)
		}

		if found1 != found2 || found2 != found3 {
			t.Errorf("Inconsistent results for tuple %d: baseline=%v, slice=%v, preallocAll=%v",
				i, found1, found2, found3)
		}
	}

	t.Logf("Correctness verified for %d tuples across 3 hydration methods", len(tuples))
}

// BenchmarkHydrationDetailed provides more detailed breakdown for a single scale.
func BenchmarkHydrationDetailed(b *testing.B) {
	ctx := context.Background()
	connStr, cleanup := setupPostgres(ctx, b)
	defer cleanup()

	store, err := infrapostgres.NewStore(ctx, connStr)
	if err != nil {
		b.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	if err := store.EnsureSchema(ctx); err != nil {
		b.Fatalf("failed to create schema: %v", err)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		b.Fatalf("failed to create pool: %v", err)
	}
	defer pool.Close()

	schema := benchSchema()

	// Use Medium config for detailed analysis
	cfg := hydrationScaleConfigs[1] // Medium/250K

	b.Logf("Generating ~%d tuples...", cfg.TupleCount())
	tuples := generateTuples(cfg, schema)
	b.Logf("Generated %d tuples", len(tuples))

	_, err = pool.Exec(ctx, "TRUNCATE tuples")
	if err != nil {
		b.Fatalf("failed to truncate: %v", err)
	}

	if err := bulkInsertTuples(ctx, pool, tuples); err != nil {
		b.Fatalf("failed to insert tuples: %v", err)
	}

	// Measure breakdown
	b.Run("Detailed", func(b *testing.B) {
		var totalDBTime, totalHydrateTime time.Duration

		for i := 0; i < b.N; i++ {
			// Phase 1: DB load
			dbStart := time.Now()
			iter, err := store.LoadAll(ctx)
			if err != nil {
				b.Fatalf("LoadAll failed: %v", err)
			}
			loadedTuples := make([]domain.Tuple, 0, len(tuples))
			for iter.Next() {
				loadedTuples = append(loadedTuples, iter.Tuple())
			}
			if err := iter.Err(); err != nil {
				b.Fatalf("iteration error: %v", err)
			}
			iter.Close()
			totalDBTime += time.Since(dbStart)

			// Phase 2: Hydrate
			hydrateStart := time.Now()
			usersets := domain.NewMultiversionUsersets(schema)
			sliceIter := domain.NewSliceIterator(loadedTuples)
			if err := usersets.Hydrate(sliceIter); err != nil {
				b.Fatalf("Hydrate failed: %v", err)
			}
			totalHydrateTime += time.Since(hydrateStart)
		}

		avgDBTime := totalDBTime / time.Duration(b.N)
		avgHydrateTime := totalHydrateTime / time.Duration(b.N)
		totalTime := avgDBTime + avgHydrateTime
		dbPercent := float64(avgDBTime) / float64(totalTime) * 100
		hydratePercent := float64(avgHydrateTime) / float64(totalTime) * 100

		b.ReportMetric(float64(len(tuples)), "tuples")
		b.ReportMetric(float64(avgDBTime.Milliseconds()), "db_ms")
		b.ReportMetric(float64(avgHydrateTime.Milliseconds()), "hydrate_ms")
		b.ReportMetric(dbPercent, "db_%")
		b.ReportMetric(hydratePercent, "hydrate_%")

		b.Logf("DB: %v (%.1f%%), Hydrate: %v (%.1f%%), Total: %v",
			avgDBTime, dbPercent, avgHydrateTime, hydratePercent, totalTime)
	})
}

// BenchmarkLargeSELECTvsCOPY compares SELECT and COPY protocols at large scale (~2.7M tuples).
// This is the key benchmark for understanding real-world hydration performance.
func BenchmarkLargeSELECTvsCOPY(b *testing.B) {
	ctx := context.Background()
	connStr, cleanup := setupPostgres(ctx, b)
	defer cleanup()

	store, err := infrapostgres.NewStore(ctx, connStr)
	if err != nil {
		b.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	if err := store.EnsureSchema(ctx); err != nil {
		b.Fatalf("failed to create schema: %v", err)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		b.Fatalf("failed to create pool: %v", err)
	}
	defer pool.Close()

	schema := benchSchema()

	// Use Large config (~2.7M tuples)
	cfg := hydrationScaleConfigs[2] // Large/2.7M

	b.Logf("Generating ~%d tuples (this may take a while)...", cfg.TupleCount())
	tuples := generateTuples(cfg, schema)
	b.Logf("Generated %d tuples", len(tuples))

	_, err = pool.Exec(ctx, "TRUNCATE tuples")
	if err != nil {
		b.Fatalf("failed to truncate: %v", err)
	}

	b.Logf("Inserting tuples into PostgreSQL (this takes ~50 seconds)...")
	insertStart := time.Now()
	if err := bulkInsertTuples(ctx, pool, tuples); err != nil {
		b.Fatalf("failed to insert tuples: %v", err)
	}
	b.Logf("Inserted %d tuples in %v", len(tuples), time.Since(insertStart))

	tupleCount := int64(len(tuples))
	mapCapacity := int(tupleCount / 20)

	// SELECT-based (baseline streaming iterator)
	b.Run("SELECT_Iterator", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			usersets := domain.NewMultiversionUsersets(schema)
			iter, err := store.LoadAll(ctx)
			if err != nil {
				b.Fatalf("LoadAll failed: %v", err)
			}
			if err := usersets.Hydrate(iter); err != nil {
				iter.Close()
				b.Fatalf("Hydrate failed: %v", err)
			}
			iter.Close()
		}
		b.ReportMetric(float64(tupleCount), "tuples")
		b.ReportMetric(float64(tupleCount)*float64(b.N)/b.Elapsed().Seconds(), "tuples/sec")
	})

	// SELECT-based with pre-sized slice
	b.Run("SELECT_Batched", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			usersets := domain.NewMultiversionUsersetsWithCapacity(schema, mapCapacity)
			loadedTuples, err := store.LoadAllBatched(ctx, tupleCount)
			if err != nil {
				b.Fatalf("LoadAllBatched failed: %v", err)
			}
			usersets.HydrateFromSlice(loadedTuples)
		}
		b.ReportMetric(float64(tupleCount), "tuples")
		b.ReportMetric(float64(tupleCount)*float64(b.N)/b.Elapsed().Seconds(), "tuples/sec")
	})

	// COPY-based (PostgreSQL's bulk export protocol)
	b.Run("COPY", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			usersets := domain.NewMultiversionUsersetsWithCapacity(schema, mapCapacity)
			loadedTuples, err := store.LoadAllCopy(ctx, tupleCount)
			if err != nil {
				b.Fatalf("LoadAllCopy failed: %v", err)
			}
			usersets.HydrateFromSlice(loadedTuples)
		}
		b.ReportMetric(float64(tupleCount), "tuples")
		b.ReportMetric(float64(tupleCount)*float64(b.N)/b.Elapsed().Seconds(), "tuples/sec")
	})

	// Also measure DB-only times for deeper analysis
	b.Run("SELECT_DBOnly", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			iter, err := store.LoadAll(ctx)
			if err != nil {
				b.Fatalf("LoadAll failed: %v", err)
			}
			count := 0
			for iter.Next() {
				_ = iter.Tuple()
				count++
			}
			if err := iter.Err(); err != nil {
				b.Fatalf("iteration error: %v", err)
			}
			iter.Close()
		}
		b.ReportMetric(float64(tupleCount), "tuples")
		b.ReportMetric(float64(tupleCount)*float64(b.N)/b.Elapsed().Seconds(), "tuples/sec")
	})

	b.Run("COPY_DBOnly", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			loadedTuples, err := store.LoadAllCopy(ctx, tupleCount)
			if err != nil {
				b.Fatalf("LoadAllCopy failed: %v", err)
			}
			_ = len(loadedTuples) // Prevent compiler from optimizing away
		}
		b.ReportMetric(float64(tupleCount), "tuples")
		b.ReportMetric(float64(tupleCount)*float64(b.N)/b.Elapsed().Seconds(), "tuples/sec")
	})

	// Binary COPY - theoretical maximum performance
	b.Run("COPY_Binary", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			usersets := domain.NewMultiversionUsersetsWithCapacity(schema, mapCapacity)
			loadedTuples, err := store.LoadAllCopyBinary(ctx, tupleCount)
			if err != nil {
				b.Fatalf("LoadAllCopyBinary failed: %v", err)
			}
			usersets.HydrateFromSlice(loadedTuples)
		}
		b.ReportMetric(float64(tupleCount), "tuples")
		b.ReportMetric(float64(tupleCount)*float64(b.N)/b.Elapsed().Seconds(), "tuples/sec")
	})

	b.Run("COPY_Binary_DBOnly", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			loadedTuples, err := store.LoadAllCopyBinary(ctx, tupleCount)
			if err != nil {
				b.Fatalf("LoadAllCopyBinary failed: %v", err)
			}
			_ = len(loadedTuples)
		}
		b.ReportMetric(float64(tupleCount), "tuples")
		b.ReportMetric(float64(tupleCount)*float64(b.N)/b.Elapsed().Seconds(), "tuples/sec")
	})

	// Batched pipeline - overlaps DB read with hydration using batches
	const pipelineBatchSize = 10000
	b.Run("PipelinedBatched", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			usersets := domain.NewMultiversionUsersetsWithCapacity(schema, mapCapacity)

			iter, err := store.LoadAll(ctx)
			if err != nil {
				b.Fatalf("LoadAll failed: %v", err)
			}

			batchChan := make(chan []domain.Tuple, 10)
			errChan := make(chan error, 1)

			go func() {
				defer close(batchChan)
				batch := make([]domain.Tuple, 0, pipelineBatchSize)
				for iter.Next() {
					batch = append(batch, iter.Tuple())
					if len(batch) >= pipelineBatchSize {
						batchChan <- batch
						batch = make([]domain.Tuple, 0, pipelineBatchSize)
					}
				}
				if len(batch) > 0 {
					batchChan <- batch
				}
				if err := iter.Err(); err != nil {
					errChan <- err
				}
				iter.Close()
			}()

			usersets.HydrateFromBatchChannel(batchChan)

			select {
			case err := <-errChan:
				b.Fatalf("Iterator error: %v", err)
			default:
			}
		}
		b.ReportMetric(float64(tupleCount), "tuples")
		b.ReportMetric(float64(tupleCount)*float64(b.N)/b.Elapsed().Seconds(), "tuples/sec")
	})
}

// TestCOPYCorrectness verifies COPY protocol produces same results as SELECT.
func TestCOPYCorrectness(t *testing.T) {
	ctx := context.Background()
	connStr, cleanup := setupPostgres(ctx, t)
	defer cleanup()

	store, err := infrapostgres.NewStore(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	defer pool.Close()

	schema := benchSchema()
	cfg := hydrationScaleConfigs[0] // Small for fast test
	tuples := generateTuples(cfg, schema)

	_, err = pool.Exec(ctx, "TRUNCATE tuples")
	if err != nil {
		t.Fatalf("failed to truncate: %v", err)
	}

	if err := bulkInsertTuples(ctx, pool, tuples); err != nil {
		t.Fatalf("failed to insert tuples: %v", err)
	}

	tupleCount := int64(len(tuples))

	// Load via SELECT
	selectTuples, err := store.LoadAllBatched(ctx, tupleCount)
	if err != nil {
		t.Fatalf("LoadAllBatched failed: %v", err)
	}

	// Load via COPY
	copyTuples, err := store.LoadAllCopy(ctx, tupleCount)
	if err != nil {
		t.Fatalf("LoadAllCopy failed: %v", err)
	}

	// Verify counts match
	if len(selectTuples) != len(copyTuples) {
		t.Fatalf("Tuple count mismatch: SELECT=%d, COPY=%d", len(selectTuples), len(copyTuples))
	}

	// Build maps for comparison (order may differ)
	selectMap := make(map[domain.Tuple]int)
	for _, tup := range selectTuples {
		selectMap[tup]++
	}

	copyMap := make(map[domain.Tuple]int)
	for _, tup := range copyTuples {
		copyMap[tup]++
	}

	// Compare maps
	for tup, count := range selectMap {
		if copyMap[tup] != count {
			t.Errorf("Tuple mismatch: %+v SELECT=%d, COPY=%d", tup, count, copyMap[tup])
		}
	}

	for tup, count := range copyMap {
		if selectMap[tup] != count {
			t.Errorf("Extra in COPY: %+v count=%d", tup, count)
		}
	}

	t.Logf("COPY text correctness verified for %d tuples", len(tuples))

	// Also verify binary COPY
	binaryTuples, err := store.LoadAllCopyBinary(ctx, tupleCount)
	if err != nil {
		t.Fatalf("LoadAllCopyBinary failed: %v", err)
	}

	if len(binaryTuples) != len(selectTuples) {
		t.Fatalf("Binary tuple count mismatch: SELECT=%d, Binary=%d", len(selectTuples), len(binaryTuples))
	}

	binaryMap := make(map[domain.Tuple]int)
	for _, tup := range binaryTuples {
		binaryMap[tup]++
	}

	for tup, count := range selectMap {
		if binaryMap[tup] != count {
			t.Errorf("Binary mismatch: %+v SELECT=%d, Binary=%d", tup, count, binaryMap[tup])
		}
	}

	t.Logf("COPY binary correctness verified for %d tuples", len(tuples))
}
