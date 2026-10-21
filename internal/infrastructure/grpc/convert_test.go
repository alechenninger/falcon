package grpc

import (
	"math"
	"reflect"
	"testing"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/alechenninger/falcon/internal/domain"
	graphpb "github.com/alechenninger/falcon/internal/infrastructure/grpc/proto"
)

func TestSnapshotWindowFromProtoFullRange(t *testing.T) {
	got := SnapshotWindowFromProto(&graphpb.SnapshotWindow{Max: math.MaxUint64})
	if got.Min() != 0 || uint64(got.Max()) != math.MaxUint64 {
		t.Fatalf("SnapshotWindowFromProto(full range) = %v", got)
	}
}

func TestSchemaIDsFromProto(t *testing.T) {
	visited := []*graphpb.VisitedNode{{ObjectTypeId: 256, ObjectId: 42, RelationId: 32767}}
	gotVisited, err := VisitedKeysFromProto(visited)
	if err != nil {
		t.Fatal(err)
	}
	if got := VisitedKeysToProto(gotVisited); !reflect.DeepEqual(got, visited) {
		t.Fatalf("visited IDs changed: got %v, want %v", got, visited)
	}

	checks, err := RelationChecksFromProto([]*graphpb.RelationCheck{{ObjectTypeId: 32767, RelationId: 256}})
	if err != nil {
		t.Fatal(err)
	}
	if uint32(checks[0].ObjectType) != 32767 || uint32(checks[0].Relation) != 256 {
		t.Fatalf("relation check IDs truncated: %+v", checks[0])
	}

	sets := []*graphpb.DependentSet{{ObjectTypeId: 257, RelationId: 16384}}
	gotSets, err := DependentSetsFromProto(sets)
	if err != nil {
		t.Fatal(err)
	}
	if got := DependentSetsToProto(gotSets); !reflect.DeepEqual(got, sets) {
		t.Fatalf("dependent set IDs changed: got %v, want %v", got, sets)
	}

	// Bitmap contents and snapshot windows still round trip with the wider IDs.
	checks[0].ObjectIDs = roaring64.BitmapOf(1, 1<<40)
	checks[0].Window = domain.NewSnapshotWindow(10, 20)
	gotChecks, err := RelationChecksFromProto(RelationChecksToProto(checks))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotChecks, checks) {
		t.Fatalf("relation checks changed: got %+v, want %+v", gotChecks, checks)
	}
}

func TestSchemaIDsFromProtoRejectsInvalidIDs(t *testing.T) {
	for _, id := range []uint32{0, 32768, 65536, math.MaxUint32} {
		for _, check := range []*graphpb.RelationCheck{
			{ObjectTypeId: id, RelationId: 1},
			{ObjectTypeId: 1, RelationId: id},
		} {
			if _, err := RelationChecksFromProto([]*graphpb.RelationCheck{check}); err == nil {
				t.Errorf("accepted invalid relation check IDs: %v", check)
			}
			visited := &graphpb.VisitedNode{ObjectTypeId: check.ObjectTypeId, RelationId: check.RelationId}
			if _, err := VisitedKeysFromProto([]*graphpb.VisitedNode{visited}); err == nil {
				t.Errorf("accepted invalid visited IDs: %v", visited)
			}
			sets := &graphpb.DependentSet{ObjectTypeId: check.ObjectTypeId, RelationId: check.RelationId}
			if _, err := DependentSetsFromProto([]*graphpb.DependentSet{sets}); err == nil {
				t.Errorf("accepted invalid dependent set IDs: %v", sets)
			}
		}
	}
}
