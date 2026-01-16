package store

import (
	"github.com/alechenninger/falcon/internal/infrastructure/postgres"
)

// PostgresStream implements ChangeStream using PostgreSQL logical replication.
// Deprecated: Use postgres.Stream instead.
type PostgresStream = postgres.Stream

// NewPostgresStream creates a new PostgresStream.
// Deprecated: Use postgres.NewStream instead.
func NewPostgresStream(connString, slotName, publication string) *PostgresStream {
	return postgres.NewStream(connString, slotName, publication)
}

// ParseLSN parses a PostgreSQL LSN string (e.g., "0/16B3748") into a StoreTime.
// Deprecated: Use postgres.ParseLSN instead.
func ParseLSN(s string) (StoreTime, error) {
	return postgres.ParseLSN(s)
}

// FormatLSN formats a StoreTime as a PostgreSQL LSN string (e.g., "0/16B3748").
// Deprecated: Use postgres.FormatLSN instead.
func FormatLSN(t StoreTime) string {
	return postgres.FormatLSN(t)
}
