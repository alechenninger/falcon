package graph

import (
	"github.com/alechenninger/falcon/schema"
	"github.com/alechenninger/falcon/store"
)

// TestDataConfig configures the deterministic graph generator.
type TestDataConfig struct {
	// NumTenants is the number of top-level folders (tenants).
	// Default: 20
	NumTenants int

	// FolderDepth is how many levels deep the folder hierarchy goes.
	// Default: 6
	FolderDepth int

	// FoldersPerLevel is how many subfolders each folder has.
	// Default: 20
	FoldersPerLevel int

	// GroupsPerTenant is how many groups each tenant has.
	// Default: 100
	GroupsPerTenant int

	// UsersPerGroup is how many users are in each group.
	// Default: 10000
	UsersPerGroup int

	// AccessTuplesPerTenant is how many group-role tuples per tenant.
	// Default: 50
	AccessTuplesPerTenant int
}

// DefaultTestDataConfig returns a config that generates 221M tuples.
func DefaultTestDataConfig() TestDataConfig {
	return TestDataConfig{
		NumTenants:            10,
		FolderDepth:           7,
		FoldersPerLevel:       10,
		GroupsPerTenant:       100,
		UsersPerGroup:         10000,
		AccessTuplesPerTenant: 50,
	}
}

// SmallTestDataConfig returns a config suitable for quick tests.
// Generates ~238 tuples.
func SmallTestDataConfig() TestDataConfig {
	return TestDataConfig{
		NumTenants:            2,
		FolderDepth:           3,
		FoldersPerLevel:       3,
		GroupsPerTenant:       5,
		UsersPerGroup:         10,
		AccessTuplesPerTenant: 3,
	}
}

// MediumTestDataConfig returns a config for medium-scale testing.
// Generates ~20K tuples - good for testing distributed behavior without waiting forever.
func MediumTestDataConfig() TestDataConfig {
	return TestDataConfig{
		NumTenants:            5,
		FolderDepth:           4,
		FoldersPerLevel:       5,
		GroupsPerTenant:       20,
		UsersPerGroup:         100,
		AccessTuplesPerTenant: 10,
	}
}

// TestDataSchema returns the schema used by the test data generator.
// This is the same schema as testSchema() in check_test.go.
//
// Type IDs:
//
//	user=1, group=2, folder=3, document=4
//
// Relation IDs (per type):
//
//	group: member=1
//	folder: parent=1, viewer=2, editor=3
//	document: parent=1, viewer=2, editor=3
func TestDataSchema() *schema.Schema {
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

// GenerateTestData generates deterministic test tuples based on the config.
// The generated data represents a multi-tenant document management system
// with deep folder hierarchies and group-based access control.
//
// Structure:
//   - NumTenants top-level folders
//   - Each folder has FoldersPerLevel subfolders, FolderDepth levels deep
//   - Each top-level folder (tenant) has GroupsPerTenant groups
//   - Each group has UsersPerGroup users
//   - Each top-level folder has AccessTuplesPerTenant group viewer/editor tuples
//   - Each leaf folder has exactly one document
func GenerateTestData(cfg TestDataConfig) []store.Tuple {
	gen := &testDataGenerator{cfg: cfg}
	return gen.generate()
}

// testDataGenerator holds state during generation.
type testDataGenerator struct {
	cfg        TestDataConfig
	tuples     []store.Tuple
	nextDoc    schema.ID
	nextFolder schema.ID
}

func (g *testDataGenerator) generate() []store.Tuple {
	// Pre-allocate slice (rough estimate)
	estimated := g.estimateTupleCount()
	g.tuples = make([]store.Tuple, 0, estimated)
	g.nextDoc = 1
	// Start folder IDs after root folders (IDs 1..NumTenants are roots)
	g.nextFolder = schema.ID(g.cfg.NumTenants + 1)

	// Generate each tenant
	for tenant := 0; tenant < g.cfg.NumTenants; tenant++ {
		g.generateTenant(tenant)
	}

	return g.tuples
}

func (g *testDataGenerator) estimateTupleCount() int {
	// Folder hierarchy: sum of (FoldersPerLevel^i) for i=1..FolderDepth, times NumTenants
	folderCount := 0
	level := 1
	for i := 0; i < g.cfg.FolderDepth; i++ {
		level *= g.cfg.FoldersPerLevel
		folderCount += level
	}
	folderCount *= g.cfg.NumTenants

	// Group memberships
	groupMemberships := g.cfg.NumTenants * g.cfg.GroupsPerTenant * g.cfg.UsersPerGroup

	// Access tuples
	accessTuples := g.cfg.NumTenants * g.cfg.AccessTuplesPerTenant

	// Documents (one per leaf folder)
	leafFolders := 1
	for i := 0; i < g.cfg.FolderDepth; i++ {
		leafFolders *= g.cfg.FoldersPerLevel
	}
	leafFolders *= g.cfg.NumTenants

	return folderCount + groupMemberships + accessTuples + leafFolders
}

func (g *testDataGenerator) generateTenant(tenant int) {
	// Root folder ID for this tenant (1-based)
	rootFolderID := schema.ID(tenant + 1)

	// Generate groups and their members
	g.generateGroups(tenant)

	// Generate access tuples (group -> folder viewer/editor)
	g.generateAccessTuples(tenant, rootFolderID)

	// Generate folder hierarchy recursively
	g.generateFolderTree(rootFolderID, 0)
}

// generateGroups creates groups and their user memberships for a tenant.
//
// ID scheme:
//   - Groups: [tenant*100 + 1, tenant*100 + GroupsPerTenant]
//   - Users: [tenant*1_000_000 + 1, tenant*1_000_000 + GroupsPerTenant*UsersPerGroup]
func (g *testDataGenerator) generateGroups(tenant int) {
	groupBase := schema.ID(tenant*100 + 1)
	userBase := schema.ID(tenant*1_000_000 + 1)

	for groupIdx := 0; groupIdx < g.cfg.GroupsPerTenant; groupIdx++ {
		groupID := groupBase + schema.ID(groupIdx)

		for userIdx := 0; userIdx < g.cfg.UsersPerGroup; userIdx++ {
			userID := userBase + schema.ID(groupIdx*g.cfg.UsersPerGroup+userIdx)
			g.tuples = append(g.tuples, store.Tuple{
				ObjectType:      "group",
				ObjectID:        groupID,
				Relation:        "member",
				SubjectType:     "user",
				SubjectID:       userID,
				SubjectRelation: "",
			})
		}
	}
}

// generateAccessTuples creates group -> folder access tuples for a tenant.
// Half are viewer, half are editor.
func (g *testDataGenerator) generateAccessTuples(tenant int, rootFolderID schema.ID) {
	groupBase := schema.ID(tenant*100 + 1)

	for i := 0; i < g.cfg.AccessTuplesPerTenant; i++ {
		groupID := groupBase + schema.ID(i%g.cfg.GroupsPerTenant)
		relation := schema.RelationName("viewer")
		if i%2 == 1 {
			relation = "editor"
		}

		g.tuples = append(g.tuples, store.Tuple{
			ObjectType:      "folder",
			ObjectID:        rootFolderID,
			Relation:        relation,
			SubjectType:     "group",
			SubjectID:       groupID,
			SubjectRelation: "member",
		})
	}
}

// generateFolderTree recursively generates the folder hierarchy.
// Each folder at depth < FolderDepth gets FoldersPerLevel children.
// Leaf folders (depth == FolderDepth) each get one document.
//
// Folder IDs are assigned sequentially to ensure uniqueness and spread across shards.
func (g *testDataGenerator) generateFolderTree(parentID schema.ID, depth int) {
	if depth >= g.cfg.FolderDepth {
		// Leaf folder - create a document
		docID := g.nextDoc
		g.nextDoc++

		g.tuples = append(g.tuples, store.Tuple{
			ObjectType:      "document",
			ObjectID:        docID,
			Relation:        "parent",
			SubjectType:     "folder",
			SubjectID:       parentID,
			SubjectRelation: "",
		})
		return
	}

	// Create child folders
	for i := 0; i < g.cfg.FoldersPerLevel; i++ {
		childID := g.nextFolder
		g.nextFolder++

		// folder:child#parent@folder:parent
		g.tuples = append(g.tuples, store.Tuple{
			ObjectType:      "folder",
			ObjectID:        childID,
			Relation:        "parent",
			SubjectType:     "folder",
			SubjectID:       parentID,
			SubjectRelation: "",
		})

		// Recurse
		g.generateFolderTree(childID, depth+1)
	}
}

// TestDataRouter returns a Router function that implements the sharding strategy
// described in the plan: folders/documents by ID, users/groups co-located by tenant.
func TestDataRouter(numShards int) Router {
	return func(objectType schema.TypeName, objectID schema.ID) ShardID {
		switch objectType {
		case "folder", "document":
			// Spread folders/docs across all shards by ID
			shardNum := int(objectID) % numShards
			return ShardID(shardIDString(shardNum))
		case "user", "group":
			// Co-locate users with their groups (same tenant)
			// Groups: [tenant*100 + 1, tenant*100 + 100]
			// Users: [tenant*1_000_000 + 1, tenant*1_000_000 + ...]
			var tenantID int
			if objectType == "group" {
				tenantID = (int(objectID) - 1) / 100
			} else { // user
				tenantID = (int(objectID) - 1) / 1_000_000
			}
			shardNum := tenantID % numShards
			return ShardID(shardIDString(shardNum))
		default:
			// Unknown type - hash to spread
			shardNum := int(objectID) % numShards
			return ShardID(shardIDString(shardNum))
		}
	}
}

// shardIDString converts a shard number to a string ID.
func shardIDString(n int) string {
	// Simple: "shard-0", "shard-1", etc.
	return "shard-" + itoa(n)
}

// itoa is a simple int to string conversion to avoid fmt import.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	if n < 0 {
		return "-" + itoa(-n)
	}
	digits := make([]byte, 0, 10)
	for n > 0 {
		digits = append(digits, byte('0'+n%10))
		n /= 10
	}
	// Reverse
	for i, j := 0, len(digits)-1; i < j; i, j = i+1, j-1 {
		digits[i], digits[j] = digits[j], digits[i]
	}
	return string(digits)
}

// FilterTuplesForShard filters tuples to only those owned by the given shard.
func FilterTuplesForShard(tuples []store.Tuple, shardID ShardID, router Router) []store.Tuple {
	result := make([]store.Tuple, 0, len(tuples)/2) // Rough estimate
	for _, t := range tuples {
		if router(t.ObjectType, t.ObjectID) == shardID {
			result = append(result, t)
		}
	}
	return result
}

// GetTestUser returns a user ID that is a member of groups for the given tenant.
// userIndex should be in [0, GroupsPerTenant*UsersPerGroup).
func GetTestUser(tenant, userIndex int) schema.ID {
	return schema.ID(tenant*1_000_000 + 1 + userIndex)
}

// GetTestGroup returns a group ID for the given tenant.
// groupIndex should be in [0, GroupsPerTenant).
func GetTestGroup(tenant, groupIndex int) schema.ID {
	return schema.ID(tenant*100 + 1 + groupIndex)
}

// GetRootFolder returns the root folder ID for the given tenant.
func GetRootFolder(tenant int) schema.ID {
	return schema.ID(tenant + 1)
}
