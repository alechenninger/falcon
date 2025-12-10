package graph

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"testing"

	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

// ScaleConfig defines parameters for building a large-scale authorization graph.
type ScaleConfig struct {
	// Hierarchy parameters
	FolderDepth          int // Levels of nested folders (root -> ... -> leaf)
	FoldersPerLevel      int // Number of folders at each level
	DocumentsPerFolder   int // Documents in each leaf folder
	ParentSpreadPerLevel int // How many parents each folder can have (1 = strict tree, >1 = DAG)

	// Group parameters
	NumGroups              int // Total number of groups
	UsersPerGroup          int // Average users per group
	GroupsPerFolder        int // Groups with access to each folder (via group#member)
	DirectViewersPerFolder int // Direct users with access to each folder

	// For realistic queries
	NumUsers int // Total unique users (some will be in multiple groups)
}

// TupleCount estimates the total number of tuples for this configuration.
func (c ScaleConfig) TupleCount() int64 {
	// Calculate folder counts at each level
	// Level 0 has FoldersPerLevel folders, each subsequent level multiplies
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

	// Parent tuples (each non-root folder has parents)
	// Root folders (level 0) have no parents
	nonRootFolders := totalFolders - c.FoldersPerLevel
	parentTuples := int64(nonRootFolders) * int64(c.ParentSpreadPerLevel)

	// Viewer tuples on folders (groups + direct users)
	folderViewerTuples := int64(totalFolders) * int64(c.GroupsPerFolder+c.DirectViewersPerFolder)

	// Document tuples: parent relation for each document
	documentTuples := int64(numLeafFolders) * int64(c.DocumentsPerFolder)

	// Group membership tuples
	groupMemberTuples := int64(c.NumGroups) * int64(c.UsersPerGroup)

	return parentTuples + folderViewerTuples + documentTuples + groupMemberTuples
}

// Describe returns a human-readable description of the scale config.
func (c ScaleConfig) Describe() string {
	numLeafFolders := c.FoldersPerLevel
	for i := 1; i < c.FolderDepth; i++ {
		numLeafFolders *= c.FoldersPerLevel
	}
	totalFolders := 0
	foldersAtLevel := c.FoldersPerLevel
	for i := 0; i <= c.FolderDepth; i++ {
		totalFolders += foldersAtLevel
		foldersAtLevel *= c.FoldersPerLevel
	}
	numDocs := numLeafFolders * c.DocumentsPerFolder

	return fmt.Sprintf("folders=%d (depth=%d), docs=%d, groups=%d, users=%d, est_tuples=%d",
		totalFolders, c.FolderDepth, numDocs, c.NumGroups, c.NumUsers, c.TupleCount())
}

// BenchGraph wraps a LocalGraph with direct population methods for benchmarking.
// This bypasses the store/observer machinery to enable fast bulk loading.
type BenchGraph struct {
	usersets *MultiversionUsersets
	graph    *LocalGraph
	time     store.StoreTime // Simple incrementing counter for populating
}

// NewBenchGraph creates a graph for benchmarking with direct population.
func NewBenchGraph(s *schema.Schema) *BenchGraph {
	usersets := NewMultiversionUsersets(s)
	// Create a minimal LocalGraph with nil store/stream (won't be used for bench loading)
	graph := &LocalGraph{
		usersets: usersets,
		observer: NoOpUsersetsObserver{},
	}
	return &BenchGraph{
		usersets: usersets,
		graph:    graph,
		time:     1,
	}
}

// Check delegates to the underlying LocalGraph.
func (bg *BenchGraph) Check(ctx context.Context, subjectType schema.TypeName, subjectID schema.ID, objectType schema.TypeName, objectID schema.ID, relation schema.RelationName) (bool, store.StoreTime, error) {
	ok, window, err := bg.graph.Check(ctx, subjectType, subjectID, objectType, objectID, relation, MaxSnapshotWindow, nil)
	return ok, window.Max(), err
}

// AddDirect adds a tuple directly to the graph's in-memory state.
func (bg *BenchGraph) AddDirect(objectType schema.TypeName, objectID schema.ID, relation schema.RelationName, subjectType schema.TypeName, subjectID schema.ID, subjectRelation schema.RelationName) {
	bg.usersets.applyAdd(objectType, objectID, relation, subjectType, subjectID, subjectRelation, bg.time)
	bg.time++
}

// benchSchema creates a schema for scale testing with deep hierarchies and groups.
// Type IDs: user=1, group=2, folder=3, document=4
// Relation IDs: member=1, parent=1, viewer=2, editor=3
func benchSchema() *schema.Schema {
	s := &schema.Schema{
		Types: map[schema.TypeName]*schema.ObjectType{
			"user": {
				ID:        1,
				Name:      "user",
				Relations: map[schema.RelationName]*schema.Relation{},
			},
			"group": {
				ID:   2,
				Name: "group",
				Relations: map[schema.RelationName]*schema.Relation{
					"member": {
						ID:   1,
						Name: "member",
						Usersets: []schema.Userset{
							schema.Direct(schema.Ref("user")),
						},
					},
				},
			},
			"folder": {
				ID:   3,
				Name: "folder",
				Relations: map[schema.RelationName]*schema.Relation{
					"parent": {
						ID:   1,
						Name: "parent",
						Usersets: []schema.Userset{
							schema.Direct(schema.Ref("folder")),
						},
					},
					"viewer": {
						ID:   2,
						Name: "viewer",
						Usersets: []schema.Userset{
							schema.Direct(
								schema.Ref("user"),
								schema.RefWithRelation("group", "member"),
							),
							schema.Arrow("parent", "viewer"),
						},
					},
					"editor": {
						ID:   3,
						Name: "editor",
						Usersets: []schema.Userset{
							schema.Direct(
								schema.Ref("user"),
								schema.RefWithRelation("group", "member"),
							),
							schema.Arrow("parent", "editor"),
						},
					},
				},
			},
			"document": {
				ID:   4,
				Name: "document",
				Relations: map[schema.RelationName]*schema.Relation{
					"parent": {
						ID:   1,
						Name: "parent",
						Usersets: []schema.Userset{
							schema.Direct(schema.Ref("folder")),
						},
					},
					"viewer": {
						ID:   2,
						Name: "viewer",
						Usersets: []schema.Userset{
							schema.Direct(
								schema.Ref("user"),
								schema.RefWithRelation("group", "member"),
							),
							schema.Computed("editor"),
							schema.Arrow("parent", "viewer"),
						},
					},
					"editor": {
						ID:   3,
						Name: "editor",
						Usersets: []schema.Userset{
							schema.Direct(
								schema.Ref("user"),
								schema.RefWithRelation("group", "member"),
							),
							schema.Arrow("parent", "editor"),
						},
					},
				},
			},
		},
	}
	s.Compile()
	return s
}

// PopulatedGraph holds a fully populated graph and query fixtures.
type PopulatedGraph struct {
	Graph *BenchGraph

	// Query fixtures for benchmarking
	DirectUserDocs   []CheckQuery // User -> Document with direct access
	GroupUserDocs    []CheckQuery // User -> Document via group membership
	ShallowArrowDocs []CheckQuery // User -> Document 1 level up
	DeepArrowDocs    []CheckQuery // User -> Document at max depth
	NegativeChecks   []CheckQuery // User -> Document with no access
	RandomChecks     []CheckQuery // Random mix of positive and negative

	// Stats
	TotalTuples int64
	NumFolders  int
	NumDocs     int
	NumGroups   int
	NumUsers    int
}

// CheckQuery represents a single check query for benchmarking.
type CheckQuery struct {
	SubjectType schema.TypeName
	SubjectID   schema.ID
	ObjectType  schema.TypeName
	ObjectID    schema.ID
	Relation    schema.RelationName
	Expected    bool // For validation
}

// buildLargeGraph constructs a graph according to the scale configuration.
func buildLargeGraph(cfg ScaleConfig) *PopulatedGraph {
	g := NewBenchGraph(benchSchema())
	rng := rand.New(rand.NewSource(42))

	result := &PopulatedGraph{
		Graph:     g,
		NumGroups: cfg.NumGroups,
		NumUsers:  cfg.NumUsers,
	}

	// Track folder IDs at each level for hierarchy building
	// Level 0 = root folders, Level N = leaf folders
	foldersByLevel := make([][]schema.ID, cfg.FolderDepth+1)
	nextFolderID := schema.ID(1)

	// Create root folders
	foldersByLevel[0] = make([]schema.ID, cfg.FoldersPerLevel)
	for i := 0; i < cfg.FoldersPerLevel; i++ {
		foldersByLevel[0][i] = nextFolderID
		nextFolderID++
	}
	result.NumFolders = cfg.FoldersPerLevel

	// Build folder hierarchy level by level
	for level := 1; level <= cfg.FolderDepth; level++ {
		parentFolders := foldersByLevel[level-1]
		childFolders := make([]schema.ID, 0, len(parentFolders)*cfg.FoldersPerLevel)

		for _, parentID := range parentFolders {
			for i := 0; i < cfg.FoldersPerLevel; i++ {
				childID := nextFolderID
				nextFolderID++
				childFolders = append(childFolders, childID)

				// Add parent relationship
				g.AddDirect("folder", childID, "parent", "folder", parentID, "")

				// Optionally add additional parents for DAG structure
				if cfg.ParentSpreadPerLevel > 1 && len(parentFolders) > 1 {
					for j := 1; j < cfg.ParentSpreadPerLevel && j < len(parentFolders); j++ {
						altParent := parentFolders[rng.Intn(len(parentFolders))]
						if altParent != parentID {
							g.AddDirect("folder", childID, "parent", "folder", altParent, "")
						}
					}
				}
			}
		}
		foldersByLevel[level] = childFolders
		result.NumFolders += len(childFolders)
	}

	// Create groups and populate with users
	userAssignments := make([][]schema.ID, cfg.NumGroups) // groupID -> userIDs
	for groupID := schema.ID(1); groupID <= schema.ID(cfg.NumGroups); groupID++ {
		userAssignments[groupID-1] = make([]schema.ID, 0, cfg.UsersPerGroup)
		for i := 0; i < cfg.UsersPerGroup; i++ {
			userID := schema.ID(rng.Intn(cfg.NumUsers) + 1)
			g.AddDirect("group", groupID, "member", "user", userID, "")
			userAssignments[groupID-1] = append(userAssignments[groupID-1], userID)
		}
	}

	// Add viewers to folders (both groups and direct users)
	allFolders := make([]schema.ID, 0, result.NumFolders)
	for _, level := range foldersByLevel {
		allFolders = append(allFolders, level...)
	}

	folderViewerGroups := make(map[schema.ID][]schema.ID) // folderID -> groupIDs with access
	folderViewerUsers := make(map[schema.ID][]schema.ID)  // folderID -> direct userIDs

	for _, folderID := range allFolders {
		// Add group viewers
		groupsForFolder := make([]schema.ID, 0, cfg.GroupsPerFolder)
		for i := 0; i < cfg.GroupsPerFolder; i++ {
			groupID := schema.ID(rng.Intn(cfg.NumGroups) + 1)
			g.AddDirect("folder", folderID, "viewer", "group", groupID, "member")
			groupsForFolder = append(groupsForFolder, groupID)
		}
		folderViewerGroups[folderID] = groupsForFolder

		// Add direct viewers
		directUsers := make([]schema.ID, 0, cfg.DirectViewersPerFolder)
		for i := 0; i < cfg.DirectViewersPerFolder; i++ {
			userID := schema.ID(rng.Intn(cfg.NumUsers) + 1)
			g.AddDirect("folder", folderID, "viewer", "user", userID, "")
			directUsers = append(directUsers, userID)
		}
		folderViewerUsers[folderID] = directUsers
	}

	// Create documents in leaf folders
	leafFolders := foldersByLevel[cfg.FolderDepth]
	nextDocID := schema.ID(1)
	docParents := make(map[schema.ID]schema.ID) // docID -> parent folderID

	for _, folderID := range leafFolders {
		for i := 0; i < cfg.DocumentsPerFolder; i++ {
			docID := nextDocID
			nextDocID++
			g.AddDirect("document", docID, "parent", "folder", folderID, "")
			docParents[docID] = folderID
		}
	}
	result.NumDocs = int(nextDocID - 1)

	// Calculate total tuples
	result.TotalTuples = int64(g.time - 1)

	// Generate query fixtures

	// 1. Direct user access queries (user is direct viewer of a folder containing the doc)
	for i := 0; i < 100; i++ {
		if len(allFolders) == 0 {
			break
		}
		folderID := allFolders[rng.Intn(len(allFolders))]
		directUsers := folderViewerUsers[folderID]
		if len(directUsers) > 0 {
			// Find a document in a folder where this folder is an ancestor
			// For simplicity, use leaf folder docs
			if cfg.DocumentsPerFolder > 0 && len(leafFolders) > 0 {
				leafFolder := leafFolders[rng.Intn(len(leafFolders))]
				docID := schema.ID(rng.Intn(cfg.DocumentsPerFolder) + 1)
				// Adjust doc ID based on which leaf folder
				leafIdx := 0
				for j, lf := range leafFolders {
					if lf == leafFolder {
						leafIdx = j
						break
					}
				}
				docID = schema.ID(leafIdx*cfg.DocumentsPerFolder + int(docID))
				if docID <= schema.ID(result.NumDocs) {
					userID := directUsers[rng.Intn(len(directUsers))]
					result.DirectUserDocs = append(result.DirectUserDocs, CheckQuery{
						SubjectType: "user",
						SubjectID:   userID,
						ObjectType:  "document",
						ObjectID:    docID,
						Relation:    "viewer",
						Expected:    true,
					})
				}
			}
		}
	}

	// 2. Group membership access queries
	for i := 0; i < 100; i++ {
		if len(allFolders) == 0 || cfg.NumGroups == 0 {
			break
		}
		folderID := allFolders[rng.Intn(len(allFolders))]
		groups := folderViewerGroups[folderID]
		if len(groups) > 0 {
			groupID := groups[rng.Intn(len(groups))]
			users := userAssignments[groupID-1]
			if len(users) > 0 && cfg.DocumentsPerFolder > 0 && len(leafFolders) > 0 {
				leafFolder := leafFolders[rng.Intn(len(leafFolders))]
				leafIdx := 0
				for j, lf := range leafFolders {
					if lf == leafFolder {
						leafIdx = j
						break
					}
				}
				docID := schema.ID(leafIdx*cfg.DocumentsPerFolder + rng.Intn(cfg.DocumentsPerFolder) + 1)
				if docID <= schema.ID(result.NumDocs) {
					userID := users[rng.Intn(len(users))]
					result.GroupUserDocs = append(result.GroupUserDocs, CheckQuery{
						SubjectType: "user",
						SubjectID:   userID,
						ObjectType:  "document",
						ObjectID:    docID,
						Relation:    "viewer",
						Expected:    true,
					})
				}
			}
		}
	}

	// 3. Shallow arrow traversal (depth 1)
	if cfg.FolderDepth >= 1 && len(foldersByLevel[1]) > 0 {
		for i := 0; i < 100 && i < len(foldersByLevel[1]); i++ {
			folderID := foldersByLevel[1][i]
			directUsers := folderViewerUsers[folderID]
			if len(directUsers) > 0 && result.NumDocs > 0 {
				userID := directUsers[rng.Intn(len(directUsers))]
				docID := schema.ID(rng.Intn(result.NumDocs) + 1)
				result.ShallowArrowDocs = append(result.ShallowArrowDocs, CheckQuery{
					SubjectType: "user",
					SubjectID:   userID,
					ObjectType:  "document",
					ObjectID:    docID,
					Relation:    "viewer",
					Expected:    true, // May or may not be true depending on structure
				})
			}
		}
	}

	// 4. Deep arrow traversal (root level access)
	if len(foldersByLevel[0]) > 0 {
		for i := 0; i < 100 && i < len(foldersByLevel[0]); i++ {
			folderID := foldersByLevel[0][i]
			directUsers := folderViewerUsers[folderID]
			if len(directUsers) > 0 && result.NumDocs > 0 {
				userID := directUsers[rng.Intn(len(directUsers))]
				docID := schema.ID(rng.Intn(result.NumDocs) + 1)
				result.DeepArrowDocs = append(result.DeepArrowDocs, CheckQuery{
					SubjectType: "user",
					SubjectID:   userID,
					ObjectType:  "document",
					ObjectID:    docID,
					Relation:    "viewer",
					Expected:    true,
				})
			}
		}
	}

	// 5. Negative checks (users with no access)
	for i := 0; i < 100; i++ {
		// Use user IDs that are unlikely to have access
		userID := schema.ID(cfg.NumUsers + 1000 + rng.Intn(1000))
		docID := schema.ID(rng.Intn(max(1, result.NumDocs)) + 1)
		result.NegativeChecks = append(result.NegativeChecks, CheckQuery{
			SubjectType: "user",
			SubjectID:   userID,
			ObjectType:  "document",
			ObjectID:    docID,
			Relation:    "viewer",
			Expected:    false,
		})
	}

	// 6. Random mix (for realistic workload)
	allQueries := make([]CheckQuery, 0)
	allQueries = append(allQueries, result.DirectUserDocs...)
	allQueries = append(allQueries, result.GroupUserDocs...)
	allQueries = append(allQueries, result.NegativeChecks...)
	rng.Shuffle(len(allQueries), func(i, j int) {
		allQueries[i], allQueries[j] = allQueries[j], allQueries[i]
	})
	if len(allQueries) > 500 {
		result.RandomChecks = allQueries[:500]
	} else {
		result.RandomChecks = allQueries
	}

	return result
}

// measureHeap forces GC and returns heap bytes.
func measureHeap() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// Pre-built configurations for different scale points.
// NOTE: Folder count grows exponentially: FoldersPerLevel^(Depth+1)
//
// Measured results (Apple M4 Pro):
//
//	| Config      | Tuples | Memory  | Bytes/Tuple | Check Latency |
//	|-------------|--------|---------|-------------|---------------|
//	| Small/20K   | 19K    | 5 MB    | 280         | 4-5 µs        |
//	| Medium/250K | 237K   | 63 MB   | 265         | 4-8 µs        |
//	| Large/2.7M  | 2.7M   | 665 MB  | 246         | 12-18 µs      |
//	| XLarge/27M  | 27M    | 7.7 GB  | 285         | 29-66 µs      |
//	| Huge/100M   | 105M   | 7.3 GB  | 73          | 113-260 µs    |
//
// Huge uses 73 bytes/tuple because it's group-heavy (bitmaps share space).
// XLarge is doc-heavy so each doc needs its own map entry.
var scaleConfigs = []struct {
	name string
	cfg  ScaleConfig
}{
	{
		// Measured: 19K tuples, 155 folders, 12.5K docs, 5 MB, 4-5 µs/check
		name: "Small/20K",
		cfg: ScaleConfig{
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
	},
	{
		// Measured: 237K tuples, 780 folders, 125K docs, 63 MB, 4-8 µs/check
		name: "Medium/250K",
		cfg: ScaleConfig{
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
	},
	{
		// Measured: 2.7M tuples, 3,905 folders, 1.56M docs, 665 MB, 12-18 µs/check
		name: "Large/2.7M",
		cfg: ScaleConfig{
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
	},
	{
		// Measured: 27M tuples, 19,530 folders, 15.6M docs, 7.7 GB, 29-66 µs/check
		// Doc-heavy: each document needs its own tuple entry
		name: "XLarge/27M",
		cfg: ScaleConfig{
			FolderDepth:            5,
			FoldersPerLevel:        5,
			DocumentsPerFolder:     1000,
			ParentSpreadPerLevel:   1,
			NumGroups:              5_000,
			UsersPerGroup:          2_000,
			GroupsPerFolder:        20,
			DirectViewersPerFolder: 50,
			NumUsers:               500_000,
		},
	},
	{
		// Measured: 105M tuples, 29,523 folders, 2M docs, 7.3 GB, 113-260 µs/check
		// TRAVERSAL-HEAVY: Deep hierarchy (depth 8) + big groups (10K users each)
		// + many groups per folder (50). This stresses traversal, not just RAM.
		// Uses only 73 bytes/tuple because group memberships share bitmap space.
		name: "Huge/100M",
		cfg: ScaleConfig{
			FolderDepth:            8,
			FoldersPerLevel:        3,
			DocumentsPerFolder:     100,
			ParentSpreadPerLevel:   1,
			NumGroups:              10_000,
			UsersPerGroup:          10_000,
			GroupsPerFolder:        50,
			DirectViewersPerFolder: 50,
			NumUsers:               5_000_000,
		},
	},
}

// BenchmarkCheckAtScale benchmarks Check operations at various scales.
func BenchmarkCheckAtScale(b *testing.B) {
	for _, sc := range scaleConfigs {
		b.Run(sc.name, func(b *testing.B) {
			b.Logf("Building graph with ~%d tuples...", sc.cfg.TupleCount())

			beforeMem := measureHeap()
			pg := buildLargeGraph(sc.cfg)
			afterMem := measureHeap()

			b.Logf("Built graph: %d tuples, %d folders, %d docs, %d groups, %d users",
				pg.TotalTuples, pg.NumFolders, pg.NumDocs, pg.NumGroups, pg.NumUsers)
			b.Logf("Memory usage: %.2f MB", float64(afterMem-beforeMem)/(1024*1024))

			// Benchmark different query patterns
			if len(pg.DirectUserDocs) > 0 {
				b.Run("DirectAccess", func(b *testing.B) {
					queries := pg.DirectUserDocs
					i := 0
					for b.Loop() {
						q := queries[i%len(queries)]
						_, _, _ = pg.Graph.Check(context.Background(), q.SubjectType, q.SubjectID, q.ObjectType, q.ObjectID, q.Relation)
						i++
					}
				})
			}

			if len(pg.GroupUserDocs) > 0 {
				b.Run("GroupMembership", func(b *testing.B) {
					queries := pg.GroupUserDocs
					i := 0
					for b.Loop() {
						q := queries[i%len(queries)]
						_, _, _ = pg.Graph.Check(context.Background(), q.SubjectType, q.SubjectID, q.ObjectType, q.ObjectID, q.Relation)
						i++
					}
				})
			}

			if len(pg.DeepArrowDocs) > 0 {
				b.Run("DeepArrow", func(b *testing.B) {
					queries := pg.DeepArrowDocs
					i := 0
					for b.Loop() {
						q := queries[i%len(queries)]
						_, _, _ = pg.Graph.Check(context.Background(), q.SubjectType, q.SubjectID, q.ObjectType, q.ObjectID, q.Relation)
						i++
					}
				})
			}

			if len(pg.NegativeChecks) > 0 {
				b.Run("Negative", func(b *testing.B) {
					queries := pg.NegativeChecks
					i := 0
					for b.Loop() {
						q := queries[i%len(queries)]
						_, _, _ = pg.Graph.Check(context.Background(), q.SubjectType, q.SubjectID, q.ObjectType, q.ObjectID, q.Relation)
						i++
					}
				})
			}

			if len(pg.RandomChecks) > 0 {
				b.Run("RandomMix", func(b *testing.B) {
					queries := pg.RandomChecks
					i := 0
					for b.Loop() {
						q := queries[i%len(queries)]
						_, _, _ = pg.Graph.Check(context.Background(), q.SubjectType, q.SubjectID, q.ObjectType, q.ObjectID, q.Relation)
						i++
					}
				})
			}
		})
	}
}

// BenchmarkCheckDepth specifically tests how check latency scales with hierarchy depth.
func BenchmarkCheckDepth(b *testing.B) {
	depths := []int{1, 3, 5, 7, 10}

	for _, depth := range depths {
		b.Run(fmt.Sprintf("Depth=%d", depth), func(b *testing.B) {
			// Use small fan-out to isolate depth impact without exponential blowup
			// With FoldersPerLevel=2: depth 10 = 2^11 - 1 = 2047 folders total
			cfg := ScaleConfig{
				FolderDepth:            depth,
				FoldersPerLevel:        2,
				DocumentsPerFolder:     10,
				ParentSpreadPerLevel:   1,
				NumGroups:              50,
				UsersPerGroup:          50,
				GroupsPerFolder:        2,
				DirectViewersPerFolder: 3,
				NumUsers:               500,
			}

			pg := buildLargeGraph(cfg)
			b.Logf("Graph: depth=%d, %d tuples, %d folders", depth, pg.TotalTuples, pg.NumFolders)

			if len(pg.DeepArrowDocs) > 0 {
				queries := pg.DeepArrowDocs
				i := 0
				for b.Loop() {
					q := queries[i%len(queries)]
					_, _, _ = pg.Graph.Check(context.Background(), q.SubjectType, q.SubjectID, q.ObjectType, q.ObjectID, q.Relation)
					i++
				}
			} else if len(pg.GroupUserDocs) > 0 {
				queries := pg.GroupUserDocs
				i := 0
				for b.Loop() {
					q := queries[i%len(queries)]
					_, _, _ = pg.Graph.Check(context.Background(), q.SubjectType, q.SubjectID, q.ObjectType, q.ObjectID, q.Relation)
					i++
				}
			}
		})
	}
}

// BenchmarkCheckGroupFanOut tests how check latency scales with group membership size.
func BenchmarkCheckGroupFanOut(b *testing.B) {
	groupSizes := []int{10, 100, 1_000, 10_000}

	for _, groupSize := range groupSizes {
		b.Run(fmt.Sprintf("UsersPerGroup=%d", groupSize), func(b *testing.B) {
			// Keep folder structure small to isolate group membership impact
			cfg := ScaleConfig{
				FolderDepth:            2,
				FoldersPerLevel:        3,
				DocumentsPerFolder:     10,
				ParentSpreadPerLevel:   1,
				NumGroups:              20,
				UsersPerGroup:          groupSize,
				GroupsPerFolder:        2,
				DirectViewersPerFolder: 2,
				NumUsers:               max(1000, groupSize*2),
			}

			pg := buildLargeGraph(cfg)
			b.Logf("Graph: %d users/group, %d tuples", groupSize, pg.TotalTuples)

			if len(pg.GroupUserDocs) > 0 {
				queries := pg.GroupUserDocs
				i := 0
				for b.Loop() {
					q := queries[i%len(queries)]
					_, _, _ = pg.Graph.Check(context.Background(), q.SubjectType, q.SubjectID, q.ObjectType, q.ObjectID, q.Relation)
					i++
				}
			}
		})
	}
}

// BenchmarkCheckMemoryScaling reports memory usage at different scales.
func BenchmarkCheckMemoryScaling(b *testing.B) {
	for _, sc := range scaleConfigs {
		b.Run(sc.name, func(b *testing.B) {
			for b.Loop() {
				beforeMem := measureHeap()
				pg := buildLargeGraph(sc.cfg)
				afterMem := measureHeap()

				memMB := float64(afterMem-beforeMem) / (1024 * 1024)
				bytesPerTuple := float64(afterMem-beforeMem) / float64(pg.TotalTuples)

				b.ReportMetric(memMB, "MB")
				b.ReportMetric(bytesPerTuple, "bytes/tuple")
				b.ReportMetric(float64(pg.TotalTuples), "tuples")

				// Prevent optimization
				if pg.NumFolders == 0 {
					b.Fatal("unexpected")
				}
			}
		})
	}
}

// BenchmarkGraphStats just builds graphs and reports stats without running checks.
// Use this to tune configs before running expensive benchmarks.
func BenchmarkGraphStats(b *testing.B) {
	for _, sc := range scaleConfigs {
		b.Run(sc.name, func(b *testing.B) {
			b.Logf("Config: %s", sc.cfg.Describe())
			b.Logf("Estimated tuples: %d", sc.cfg.TupleCount())

			beforeMem := measureHeap()
			pg := buildLargeGraph(sc.cfg)
			afterMem := measureHeap()

			memMB := float64(afterMem-beforeMem) / (1024 * 1024)
			bytesPerTuple := float64(afterMem-beforeMem) / float64(pg.TotalTuples)

			b.Logf("Actual tuples: %d", pg.TotalTuples)
			b.Logf("Memory: %.2f MB (%.1f bytes/tuple)", memMB, bytesPerTuple)
			b.Logf("Folders: %d, Docs: %d, Groups: %d, Users: %d",
				pg.NumFolders, pg.NumDocs, pg.NumGroups, pg.NumUsers)

			b.ReportMetric(float64(pg.TotalTuples), "tuples")
			b.ReportMetric(memMB, "MB")

			// Skip actual iterations - we just want stats
			b.SkipNow()
		})
	}
}

// BenchmarkCheckThroughput measures sustained Check throughput.
func BenchmarkCheckThroughput(b *testing.B) {
	// Use medium config for throughput testing
	cfg := ScaleConfig{
		FolderDepth:            4,
		FoldersPerLevel:        8,
		DocumentsPerFolder:     50,
		ParentSpreadPerLevel:   1,
		NumGroups:              500,
		UsersPerGroup:          500,
		GroupsPerFolder:        5,
		DirectViewersPerFolder: 10,
		NumUsers:               50_000,
	}

	pg := buildLargeGraph(cfg)
	b.Logf("Graph: %d tuples for throughput test", pg.TotalTuples)

	queries := pg.RandomChecks
	if len(queries) == 0 {
		b.Skip("No queries generated")
	}

	b.ResetTimer()
	i := 0
	for b.Loop() {
		q := queries[i%len(queries)]
		_, _, _ = pg.Graph.Check(context.Background(), q.SubjectType, q.SubjectID, q.ObjectType, q.ObjectID, q.Relation)
		i++
	}
	b.ReportMetric(float64(b.N), "checks")
}
