package domain

import (
	"fmt"
	"testing"
	"unsafe"
)

func TestCompactIDLayouts(t *testing.T) {
	for _, tt := range []struct {
		name string
		got  uintptr
		want uintptr
	}{
		{"TypeID", unsafe.Sizeof(TypeID(0)), 2},
		{"RelationID", unsafe.Sizeof(RelationID(0)), 2},
		{"Tuple", unsafe.Sizeof(Tuple{}), 24},
		{"usersetKey", unsafe.Sizeof(usersetKey{}), 16},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("size = %d bytes, want %d", tt.got, tt.want)
			}
		})
	}
}

func TestSchemaCompileIDRange(t *testing.T) {
	for _, id := range []uint16{1, 255, 256, 257, 16384, 32767} {
		t.Run(fmt.Sprint(id), func(t *testing.T) {
			rel := &Relation{ID: RelationID(id), Name: "viewer"}
			ot := &ObjectType{ID: TypeID(id), Name: "document", Relations: map[RelationName]*Relation{"viewer": rel}}
			s := &Schema{Types: map[TypeName]*ObjectType{"document": ot}}
			s.Compile()
			if uint16(ot.ID) != id || uint16(rel.ID) != id {
				t.Fatalf("IDs truncated: type = %d, relation = %d, want %d", ot.ID, rel.ID, id)
			}
			if s.TypeByID(TypeID(id)) != ot || ot.RelationByID(RelationID(id)) != rel {
				t.Fatal("compiled schema did not preserve ID lookups")
			}
		})
	}
}

func TestSchemaCompileRejectsInvalidIDs(t *testing.T) {
	for _, id := range []uint16{0, 32768, 65535} {
		for _, field := range []string{"type", "relation"} {
			t.Run(fmt.Sprintf("%s/%d", field, id), func(t *testing.T) {
				rel := &Relation{ID: 1, Name: "viewer"}
				ot := &ObjectType{ID: 1, Name: "document", Relations: map[RelationName]*Relation{"viewer": rel}}
				if field == "type" {
					ot.ID = TypeID(id)
				} else {
					rel.ID = RelationID(id)
				}
				s := &Schema{Types: map[TypeName]*ObjectType{"document": ot}}
				defer func() {
					if recover() == nil {
						t.Fatal("Compile accepted an invalid schema ID")
					}
				}()
				s.Compile()
			})
		}
	}
}
