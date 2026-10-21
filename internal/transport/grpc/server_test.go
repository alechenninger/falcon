package grpc

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/alechenninger/falcon/internal/domain"
	infragrpc "github.com/alechenninger/falcon/internal/infrastructure/grpc"
	graphpb "github.com/alechenninger/falcon/internal/infrastructure/grpc/proto"
	"github.com/alechenninger/falcon/internal/infrastructure/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestServerSchemaIDs(t *testing.T) {
	ctx := context.Background()
	schema := &domain.Schema{Types: map[domain.TypeName]*domain.ObjectType{
		"user": {ID: 256, Name: "user"},
		"document": {
			ID: domain.MaxTypeID, Name: "document",
			Relations: map[domain.RelationName]*domain.Relation{
				"viewer": {ID: domain.MaxRelationID, Name: "viewer", Usersets: []domain.Userset{domain.Direct(domain.Ref("user"))}},
			},
		},
	}}
	schema.Compile()
	graph := domain.NewTestGraph(schema, memory.NewStore())
	t.Cleanup(graph.Close)
	if err := graph.WriteTuple(ctx, "document", 100, "viewer", "user", 1, ""); err != nil {
		t.Fatal(err)
	}
	server := NewServer(graph.LocalGraph)
	checkRequest := func() *graphpb.CheckRequest {
		return &graphpb.CheckRequest{SubjectTypeId: 256, SubjectId: 1, ObjectTypeId: 32767, ObjectId: 100, RelationId: 32767}
	}
	unionRequest := func() *graphpb.CheckUnionRequest {
		return &graphpb.CheckUnionRequest{
			SubjectTypeId: 256, SubjectId: 1,
			Checks: infragrpc.RelationChecksToProto([]domain.RelationCheck{{
				ObjectType: domain.MaxTypeID, Relation: domain.MaxRelationID,
				ObjectIDs: roaring64.BitmapOf(100), Window: domain.MaxSnapshotWindow,
			}}),
		}
	}

	if resp, err := server.Check(ctx, checkRequest()); err != nil || !resp.Allowed {
		t.Fatalf("Check = %v, %v; want allowed", resp, err)
	}
	resp, err := server.CheckUnion(ctx, unionRequest())
	if err != nil || !resp.Allowed {
		t.Fatalf("CheckUnion = %v, %v; want allowed", resp, err)
	}
	if len(resp.DependentSets) != 1 || resp.DependentSets[0].ObjectTypeId != 32767 || resp.DependentSets[0].RelationId != 32767 {
		t.Fatalf("dependent set IDs changed: %v", resp.DependentSets)
	}

	for _, id := range []uint32{0, 32768, 65536, math.MaxUint32} {
		for _, tt := range []struct {
			name string
			set  func(*graphpb.CheckRequest)
		}{
			{"subject", func(req *graphpb.CheckRequest) { req.SubjectTypeId = id }},
			{"object", func(req *graphpb.CheckRequest) { req.ObjectTypeId = id }},
			{"relation", func(req *graphpb.CheckRequest) { req.RelationId = id }},
			{"visited_type", func(req *graphpb.CheckRequest) {
				req.Visited = []*graphpb.VisitedNode{{ObjectTypeId: id, RelationId: 1}}
			}},
			{"visited_relation", func(req *graphpb.CheckRequest) {
				req.Visited = []*graphpb.VisitedNode{{ObjectTypeId: 1, RelationId: id}}
			}},
		} {
			t.Run(fmt.Sprintf("Check/%s/%d", tt.name, id), func(t *testing.T) {
				req := checkRequest()
				tt.set(req)
				if _, err := server.Check(ctx, req); status.Code(err) != codes.InvalidArgument {
					t.Fatalf("error = %v, want InvalidArgument", err)
				}
			})
		}
		for _, tt := range []struct {
			name string
			set  func(*graphpb.CheckUnionRequest)
		}{
			{"subject", func(req *graphpb.CheckUnionRequest) { req.SubjectTypeId = id }},
			{"object", func(req *graphpb.CheckUnionRequest) { req.Checks[0].ObjectTypeId = id }},
			{"relation", func(req *graphpb.CheckUnionRequest) { req.Checks[0].RelationId = id }},
			{"visited_type", func(req *graphpb.CheckUnionRequest) {
				req.Visited = []*graphpb.VisitedNode{{ObjectTypeId: id, RelationId: 1}}
			}},
			{"visited_relation", func(req *graphpb.CheckUnionRequest) {
				req.Visited = []*graphpb.VisitedNode{{ObjectTypeId: 1, RelationId: id}}
			}},
		} {
			t.Run(fmt.Sprintf("CheckUnion/%s/%d", tt.name, id), func(t *testing.T) {
				req := unionRequest()
				tt.set(req)
				if _, err := server.CheckUnion(ctx, req); status.Code(err) != codes.InvalidArgument {
					t.Fatalf("error = %v, want InvalidArgument", err)
				}
			})
		}
	}
}
