package postgres

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/alechenninger/falcon/internal/domain"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestPredicateSchemaIDEncoding(t *testing.T) {
	for _, id := range []uint16{256, 32767, 32768, 65535} {
		for _, predicate := range []domain.TuplePredicate{
			domain.Eq(domain.FieldObjectType, domain.TypeID(id)),
			domain.Eq(domain.FieldSubjectType, domain.TypeID(id)),
			domain.Eq(domain.FieldRelation, domain.RelationID(id)),
			domain.Eq(domain.FieldSubjectRelation, domain.RelationID(id)),
			domain.Eq(domain.FieldObjectID, domain.ObjectRef{Type: domain.TypeID(id), ID: "doc"}),
			domain.Eq(domain.FieldSubjectID, domain.ObjectRef{Type: domain.TypeID(id), ID: "user"}),
		} {
			t.Run(fmt.Sprintf("%v/%d", predicate, id), func(t *testing.T) {
				_, args := predicateToSQL(predicate, 1)
				encoded, err := pgtype.NewMap().Encode(pgtype.Int2OID, pgtype.BinaryFormatCode, args[0], nil)
				if id > 32767 {
					if err == nil {
						t.Fatal("out of range ID encoded without error")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if got := binary.BigEndian.Uint16(encoded); got != id {
					t.Fatalf("encoded ID = %d, want %d", got, id)
				}
			})
		}
	}
}

func TestDecodeTupleSchemaIDs(t *testing.T) {
	want := domain.Tuple{
		ObjectType: 256, ObjectID: 1<<40 + 1, Relation: 257,
		SubjectType: 32767, SubjectID: 1<<40 + 2, SubjectRelation: 16384,
	}
	relation := &pglogrepl.RelationMessage{}
	data := &pglogrepl.TupleData{}
	for _, field := range []struct {
		name  string
		value any
	}{
		{"object_type", want.ObjectType},
		{"object_id", want.ObjectID},
		{"relation", want.Relation},
		{"subject_type", want.SubjectType},
		{"subject_id", want.SubjectID},
		{"subject_relation", want.SubjectRelation},
	} {
		relation.Columns = append(relation.Columns, &pglogrepl.RelationMessageColumn{Name: field.name})
		data.Columns = append(data.Columns, &pglogrepl.TupleDataColumn{DataType: 't', Data: []byte(fmt.Sprint(field.value))})
	}
	got, err := (&Stream{}).decodeTuple(relation, data)
	if err != nil {
		t.Fatal(err)
	}
	if *got != want {
		t.Fatalf("decoded tuple = %+v, want %+v", *got, want)
	}
}
