// Package grpc provides gRPC infrastructure for Falcon.
package grpc

import (
	"bytes"
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/alechenninger/falcon/internal/domain"
	graphpb "github.com/alechenninger/falcon/internal/infrastructure/grpc/proto"
)

// TypeIDFromProto converts a protobuf type ID, rejecting unsupported values.
func TypeIDFromProto(id uint32) (domain.TypeID, error) {
	if id == 0 || id > uint32(domain.MaxTypeID) {
		return 0, fmt.Errorf("type ID %d must be between 1 and %d", id, domain.MaxTypeID)
	}
	return domain.TypeID(id), nil
}

// RelationIDFromProto converts a protobuf relation ID, rejecting unsupported values.
func RelationIDFromProto(id uint32) (domain.RelationID, error) {
	if id == 0 || id > uint32(domain.MaxRelationID) {
		return 0, fmt.Errorf("relation ID %d must be between 1 and %d", id, domain.MaxRelationID)
	}
	return domain.RelationID(id), nil
}

// byteReader wraps a byte slice to implement io.Reader.
type byteReader struct {
	data []byte
	pos  int
}

func newByteReader(data []byte) *byteReader {
	return &byteReader{data: data}
}

func (r *byteReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, nil
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// SnapshotWindowToProto converts a domain.SnapshotWindow to its proto representation.
func SnapshotWindowToProto(w domain.SnapshotWindow) *graphpb.SnapshotWindow {
	return &graphpb.SnapshotWindow{
		Min: uint64(w.Min()),
		Max: uint64(w.Max()),
	}
}

// SnapshotWindowFromProto converts a proto SnapshotWindow to the Go type.
// If the window is nil or has zero values, defaults to MaxSnapshotWindow.
func SnapshotWindowFromProto(w *graphpb.SnapshotWindow) domain.SnapshotWindow {
	if w == nil {
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
			ObjectId:     uint64(v.ObjectID),
			RelationId:   uint32(v.Relation),
		}
	}
	return result
}

// VisitedKeysFromProto converts proto VisitedNodes to a slice of domain.VisitedKey.
// It returns an error if a type or relation ID is outside the supported range.
func VisitedKeysFromProto(visited []*graphpb.VisitedNode) ([]domain.VisitedKey, error) {
	if visited == nil {
		return nil, nil
	}
	result := make([]domain.VisitedKey, len(visited))
	for i, v := range visited {
		objectType, err := TypeIDFromProto(v.GetObjectTypeId())
		if err != nil {
			return nil, err
		}
		relation, err := RelationIDFromProto(v.GetRelationId())
		if err != nil {
			return nil, err
		}
		result[i] = domain.VisitedKey{
			ObjectType: objectType,
			ObjectID:   domain.ID(v.ObjectId),
			Relation:   relation,
		}
	}
	return result, nil
}

// RelationChecksToProto converts a slice of domain.RelationCheck to proto representation.
func RelationChecksToProto(checks []domain.RelationCheck) []*graphpb.RelationCheck {
	result := make([]*graphpb.RelationCheck, len(checks))
	for i, c := range checks {
		var objectIDs []byte
		if c.ObjectIDs != nil {
			var buf bytes.Buffer
			c.ObjectIDs.WriteTo(&buf)
			objectIDs = buf.Bytes()
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
// It returns an error if a schema ID or serialized bitmap is invalid.
func RelationChecksFromProto(checks []*graphpb.RelationCheck) ([]domain.RelationCheck, error) {
	result := make([]domain.RelationCheck, len(checks))
	for i, c := range checks {
		objectType, err := TypeIDFromProto(c.GetObjectTypeId())
		if err != nil {
			return nil, err
		}
		relation, err := RelationIDFromProto(c.GetRelationId())
		if err != nil {
			return nil, err
		}
		bitmap := roaring64.New()
		if len(c.ObjectIds) > 0 {
			if _, err := bitmap.ReadFrom(newByteReader(c.ObjectIds)); err != nil {
				return nil, err
			}
		}
		result[i] = domain.RelationCheck{
			ObjectType: objectType,
			ObjectIDs:  bitmap,
			Relation:   relation,
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
			var buf bytes.Buffer
			s.ObjectIDs.WriteTo(&buf)
			objectIDs = buf.Bytes()
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
// It returns an error if a type or relation ID is outside the supported range.
func DependentSetsFromProto(sets []*graphpb.DependentSet) ([]domain.DependentSet, error) {
	if sets == nil {
		return nil, nil
	}
	result := make([]domain.DependentSet, len(sets))
	for i, s := range sets {
		objectType, err := TypeIDFromProto(s.GetObjectTypeId())
		if err != nil {
			return nil, err
		}
		relation, err := RelationIDFromProto(s.GetRelationId())
		if err != nil {
			return nil, err
		}
		var bitmap *roaring64.Bitmap
		if len(s.ObjectIds) > 0 {
			bitmap = roaring64.New()
			bitmap.ReadFrom(newByteReader(s.ObjectIds))
		}
		result[i] = domain.DependentSet{
			ObjectType: objectType,
			Relation:   relation,
			ObjectIDs:  bitmap,
		}
	}
	return result, nil
}
