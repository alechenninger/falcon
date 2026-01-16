// Package grpc provides gRPC infrastructure for Falcon.
package grpc

import (
	"math"

	"github.com/RoaringBitmap/roaring"
	"github.com/alechenninger/falcon/internal/domain"
	graphpb "github.com/alechenninger/falcon/internal/infrastructure/grpc/proto"
)

// SnapshotWindowToProto converts a domain.SnapshotWindow to its proto representation.
func SnapshotWindowToProto(w domain.SnapshotWindow) *graphpb.SnapshotWindow {
	return &graphpb.SnapshotWindow{
		Min: uint64(w.Min()),
		Max: uint64(w.Max()),
	}
}

// SnapshotWindowFromProto converts a proto SnapshotWindow to the Go type.
// Handles the special case of MaxSnapshotWindow (min=0, max=MaxUint64).
// If the window is nil or has zero values, defaults to MaxSnapshotWindow.
func SnapshotWindowFromProto(w *graphpb.SnapshotWindow) domain.SnapshotWindow {
	if w == nil {
		return domain.MaxSnapshotWindow
	}
	// Detect MaxSnapshotWindow: min=0 and max=MaxUint64
	// Can't use NewSnapshotWindow for this because delta would overflow
	if w.Min == 0 && w.Max == math.MaxUint64 {
		return domain.MaxSnapshotWindow
	}
	// TODO: reconsider this
	// Zero window (min=0, max=0) also means "use MaxSnapshotWindow"
	// since proto3 doesn't distinguish between "not set" and "zero"
	if w.Min == 0 && w.Max == 0 {
		return domain.MaxSnapshotWindow
	}
	return domain.NewSnapshotWindow(domain.StoreTime(w.Min), domain.StoreTime(w.Max))
}

// VisitedKeysToProto converts a slice of domain.VisitedKey to proto representation.
func VisitedKeysToProto(visited []domain.VisitedKey) []*graphpb.VisitedNode {
	if visited == nil {
		return nil
	}
	result := make([]*graphpb.VisitedNode, len(visited))
	for i, v := range visited {
		result[i] = &graphpb.VisitedNode{
			ObjectTypeId: uint32(v.ObjectType),
			ObjectId:     uint32(v.ObjectID),
			RelationId:   uint32(v.Relation),
		}
	}
	return result
}

// VisitedKeysFromProto converts proto VisitedNodes to a slice of domain.VisitedKey.
func VisitedKeysFromProto(visited []*graphpb.VisitedNode) []domain.VisitedKey {
	if visited == nil {
		return nil
	}
	result := make([]domain.VisitedKey, len(visited))
	for i, v := range visited {
		result[i] = domain.VisitedKey{
			ObjectType: domain.TypeID(v.ObjectTypeId),
			ObjectID:   domain.ID(v.ObjectId),
			Relation:   domain.RelationID(v.RelationId),
		}
	}
	return result
}

// RelationChecksToProto converts a slice of domain.RelationCheck to proto representation.
func RelationChecksToProto(checks []domain.RelationCheck) []*graphpb.RelationCheck {
	result := make([]*graphpb.RelationCheck, len(checks))
	for i, c := range checks {
		var objectIDs []byte
		if c.ObjectIDs != nil {
			objectIDs, _ = c.ObjectIDs.ToBytes()
		}
		result[i] = &graphpb.RelationCheck{
			ObjectTypeId: uint32(c.ObjectType),
			ObjectIds:    objectIDs,
			RelationId:   uint32(c.Relation),
			Window:       SnapshotWindowToProto(c.Window),
		}
	}
	return result
}

// RelationChecksFromProto converts proto RelationChecks to a slice of domain.RelationCheck.
func RelationChecksFromProto(checks []*graphpb.RelationCheck) ([]domain.RelationCheck, error) {
	result := make([]domain.RelationCheck, len(checks))
	for i, c := range checks {
		bitmap := roaring.New()
		if len(c.ObjectIds) > 0 {
			if _, err := bitmap.FromBuffer(c.ObjectIds); err != nil {
				return nil, err
			}
		}
		result[i] = domain.RelationCheck{
			ObjectType: domain.TypeID(c.ObjectTypeId),
			ObjectIDs:  bitmap,
			Relation:   domain.RelationID(c.RelationId),
			Window:     SnapshotWindowFromProto(c.Window),
		}
	}
	return result, nil
}

// DependentSetsToProto converts domain.DependentSets to proto representation.
func DependentSetsToProto(sets []domain.DependentSet) []*graphpb.DependentSet {
	if sets == nil {
		return nil
	}
	result := make([]*graphpb.DependentSet, len(sets))
	for i, s := range sets {
		var objectIDs []byte
		if s.ObjectIDs != nil {
			objectIDs, _ = s.ObjectIDs.ToBytes()
		}
		result[i] = &graphpb.DependentSet{
			ObjectTypeId: uint32(s.ObjectType),
			RelationId:   uint32(s.Relation),
			ObjectIds:    objectIDs,
		}
	}
	return result
}

// DependentSetsFromProto converts proto DependentSets to Go type.
func DependentSetsFromProto(sets []*graphpb.DependentSet) []domain.DependentSet {
	if sets == nil {
		return nil
	}
	result := make([]domain.DependentSet, len(sets))
	for i, s := range sets {
		var bitmap *roaring.Bitmap
		if len(s.ObjectIds) > 0 {
			bitmap = roaring.New()
			bitmap.FromBuffer(s.ObjectIds)
		}
		result[i] = domain.DependentSet{
			ObjectType: domain.TypeID(s.ObjectTypeId),
			Relation:   domain.RelationID(s.RelationId),
			ObjectIDs:  bitmap,
		}
	}
	return result
}
