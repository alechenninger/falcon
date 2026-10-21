package memory

import (
	"testing"
	"unsafe"
)

func TestTupleKeySize(t *testing.T) {
	if got := unsafe.Sizeof(tupleKey{}); got != 24 {
		t.Fatalf("tupleKey size = %d bytes, want 24", got)
	}
}
