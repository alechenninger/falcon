package grpc

import (
	"math"
	"testing"

	graphpb "github.com/alechenninger/falcon/internal/infrastructure/grpc/proto"
)

func TestSnapshotWindowFromProtoFullRange(t *testing.T) {
	got := SnapshotWindowFromProto(&graphpb.SnapshotWindow{Max: math.MaxUint64})
	if got.Min() != 0 || uint64(got.Max()) != math.MaxUint64 {
		t.Fatalf("SnapshotWindowFromProto(full range) = %v", got)
	}
}
