package memory_test

import (
	"testing"

	"github.com/alechenninger/falcon/internal/domain"
	"github.com/alechenninger/falcon/internal/infrastructure/memory"
)

func TestMemoryStore(t *testing.T) {
	domain.RunStoreTests(t, func(t *testing.T) domain.Store {
		return memory.NewStore()
	})
}
