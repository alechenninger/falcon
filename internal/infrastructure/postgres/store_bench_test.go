package postgres

// This file contains experimental load methods used only for benchmarking.
// These methods were tested for performance but did not outperform the baseline
// streaming iterator approach (LoadAll). They are preserved here for:
// - Future reference and experimentation
// - Reproducible benchmark comparisons
// - Documentation of approaches that were tried

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/alechenninger/falcon/internal/domain"
)

// TupleCount returns the number of tuples in the database.
// This is useful for pre-sizing data structures before hydration.
func (s *Store) TupleCount(ctx context.Context) (int64, error) {
	var count int64
	err := s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM tuples").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count tuples: %w", err)
	}
	return count, nil
}

// LoadAllBatched returns all tuples loaded into a pre-allocated slice.
// The hypothesis was this would be faster than the iterator approach due to:
// - Single allocation with known capacity
// - Better CPU cache locality from contiguous slice
//
// expectedCount should be the result of TupleCount().
//
// Benchmark result: ~10% slower than streaming iterator. The COUNT query adds
// latency and pre-allocation doesn't help because pgx already buffers efficiently.
func (s *Store) LoadAllBatched(ctx context.Context, expectedCount int64) ([]domain.Tuple, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT object_type, object_id, relation, subject_type, subject_id, subject_relation
		FROM tuples
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query tuples: %w", err)
	}
	defer rows.Close()

	tuples := make([]domain.Tuple, 0, expectedCount)

	for rows.Next() {
		var (
			objectType      int16
			objectID        int64
			relation        int16
			subjectType     int16
			subjectID       int64
			subjectRelation int16
		)
		if err := rows.Scan(&objectType, &objectID, &relation, &subjectType, &subjectID, &subjectRelation); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		tuples = append(tuples, domain.Tuple{
			ObjectType:      domain.TypeID(objectType),
			ObjectID:        domain.ID(objectID),
			Relation:        domain.RelationID(relation),
			SubjectType:     domain.TypeID(subjectType),
			SubjectID:       domain.ID(subjectID),
			SubjectRelation: domain.RelationID(subjectRelation),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during iteration: %w", err)
	}

	return tuples, nil
}

// LoadAllCopy returns all tuples using PostgreSQL's COPY protocol.
// The hypothesis was COPY would be faster than SELECT due to:
// - Minimal protocol overhead per row
// - No query planning per row
// - Streaming text directly from storage
//
// This uses text format which requires parsing in Go.
//
// Benchmark result: ~6% faster at the DB/wire level, but Go-side text parsing
// negates those gains. Overall ~6% slower end-to-end than streaming iterator,
// because pgx's row parsing is highly optimized.
func (s *Store) LoadAllCopy(ctx context.Context, expectedCount int64) ([]domain.Tuple, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Collect all COPY output into a buffer first (avoids pipe overhead)
	var buf copyBuffer
	buf.tuples = make([]domain.Tuple, 0, expectedCount)

	// Execute COPY TO STDOUT - writes directly to our buffer
	_, err = conn.Conn().PgConn().CopyTo(ctx, &buf,
		"COPY tuples (object_type, object_id, relation, subject_type, subject_id, subject_relation) TO STDOUT")

	if err != nil {
		return nil, fmt.Errorf("COPY failed: %w", err)
	}

	// Process any remaining data in the line buffer
	if buf.lineLen > 0 {
		tuple, err := parseCopyLineBytes(buf.lineBuf[:buf.lineLen])
		if err != nil {
			return nil, fmt.Errorf("failed to parse final line: %w", err)
		}
		buf.tuples = append(buf.tuples, tuple)
	}

	if buf.err != nil {
		return nil, buf.err
	}

	return buf.tuples, nil
}

// copyBuffer is an io.Writer that parses COPY output directly as it's written.
// This avoids the overhead of io.Pipe and bufio.Scanner.
type copyBuffer struct {
	tuples  []domain.Tuple
	lineBuf [256]byte // Fixed buffer for current line (our rows are ~30 bytes)
	lineLen int
	err     error
}

func (b *copyBuffer) Write(p []byte) (n int, err error) {
	n = len(p)
	for len(p) > 0 {
		// Find newline
		idx := -1
		for i := 0; i < len(p); i++ {
			if p[i] == '\n' {
				idx = i
				break
			}
		}

		if idx == -1 {
			// No newline, accumulate in line buffer
			if b.lineLen+len(p) > len(b.lineBuf) {
				b.err = fmt.Errorf("line too long")
				return n, b.err
			}
			copy(b.lineBuf[b.lineLen:], p)
			b.lineLen += len(p)
			return n, nil
		}

		// Complete line - parse it
		if b.lineLen+idx > len(b.lineBuf) {
			b.err = fmt.Errorf("line too long")
			return n, b.err
		}
		copy(b.lineBuf[b.lineLen:], p[:idx])
		b.lineLen += idx

		tuple, err := parseCopyLineBytes(b.lineBuf[:b.lineLen])
		if err != nil {
			b.err = fmt.Errorf("failed to parse COPY line: %w", err)
			return n, b.err
		}
		b.tuples = append(b.tuples, tuple)

		b.lineLen = 0
		p = p[idx+1:]
	}
	return n, nil
}

// parseCopyLineBytes parses a tab-separated COPY output line into a Tuple.
// Operates directly on bytes without string conversion for speed.
func parseCopyLineBytes(line []byte) (domain.Tuple, error) {
	var t domain.Tuple
	var fieldStart int
	fieldNum := 0

	for i := 0; i <= len(line); i++ {
		if i == len(line) || line[i] == '\t' {
			field := line[fieldStart:i]
			switch fieldNum {
			case 0:
				v, err := parseIntBytes(field)
				if err != nil {
					return t, fmt.Errorf("invalid object_type: %w", err)
				}
				t.ObjectType = domain.TypeID(v)
			case 1:
				v, err := parseIntBytes(field)
				if err != nil {
					return t, fmt.Errorf("invalid object_id: %w", err)
				}
				t.ObjectID = domain.ID(v)
			case 2:
				v, err := parseIntBytes(field)
				if err != nil {
					return t, fmt.Errorf("invalid relation: %w", err)
				}
				t.Relation = domain.RelationID(v)
			case 3:
				v, err := parseIntBytes(field)
				if err != nil {
					return t, fmt.Errorf("invalid subject_type: %w", err)
				}
				t.SubjectType = domain.TypeID(v)
			case 4:
				v, err := parseIntBytes(field)
				if err != nil {
					return t, fmt.Errorf("invalid subject_id: %w", err)
				}
				t.SubjectID = domain.ID(v)
			case 5:
				v, err := parseIntBytes(field)
				if err != nil {
					return t, fmt.Errorf("invalid subject_relation: %w", err)
				}
				t.SubjectRelation = domain.RelationID(v)
			}
			fieldNum++
			fieldStart = i + 1
		}
	}

	if fieldNum != 6 {
		return t, fmt.Errorf("expected 6 fields, got %d", fieldNum)
	}

	return t, nil
}

// parseIntBytes parses an integer from bytes without allocation.
func parseIntBytes(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, fmt.Errorf("empty field")
	}

	neg := false
	i := 0
	if b[0] == '-' {
		neg = true
		i = 1
	}

	var n int64
	for ; i < len(b); i++ {
		c := b[i]
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid character %c", c)
		}
		n = n*10 + int64(c-'0')
	}

	if neg {
		n = -n
	}
	return n, nil
}

// LoadAllCopyBinary returns all tuples using PostgreSQL's binary COPY protocol.
// The hypothesis was binary COPY would avoid text parsing overhead entirely,
// with integers read directly from the wire format.
//
// Binary COPY format:
// - Header: "PGCOPY\n\377\r\n\0" (11 bytes) + flags (4 bytes) + extension (4 bytes)
// - Each row: field_count (2 bytes) + for each field: length (4 bytes) + data
// - Trailer: field_count = -1 (2 bytes)
//
// Benchmark result: Did not outperform text COPY. The binary wire format is actually
// larger than text for small integers because each field has a 4-byte length prefix,
// while text format for small integers like "1" or "42" is just 1-2 bytes.
func (s *Store) LoadAllCopyBinary(ctx context.Context, expectedCount int64) ([]domain.Tuple, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Collect all binary COPY output
	var buf bytes.Buffer
	buf.Grow(int(expectedCount) * 60) // ~56 bytes per row + overhead

	_, err = conn.Conn().PgConn().CopyTo(ctx, &buf,
		"COPY tuples (object_type, object_id, relation, subject_type, subject_id, subject_relation) TO STDOUT (FORMAT binary)")
	if err != nil {
		return nil, fmt.Errorf("COPY failed: %w", err)
	}

	return parseBinaryCopy(buf.Bytes(), expectedCount)
}

// parseBinaryCopy parses PostgreSQL binary COPY format into tuples.
// Uses direct integer reads with no per-row allocations.
func parseBinaryCopy(data []byte, expectedCount int64) ([]domain.Tuple, error) {
	if len(data) < 19 {
		return nil, fmt.Errorf("data too short for binary COPY header")
	}

	// Verify header signature: "PGCOPY\n\377\r\n\0"
	signature := []byte{'P', 'G', 'C', 'O', 'P', 'Y', '\n', 0xff, '\r', '\n', 0}
	if !bytes.Equal(data[:11], signature) {
		return nil, fmt.Errorf("invalid binary COPY signature")
	}

	// Skip flags (4 bytes) and header extension length (4 bytes)
	// flags := binary.BigEndian.Uint32(data[11:15])
	extLen := binary.BigEndian.Uint32(data[15:19])
	pos := 19 + int(extLen)

	tuples := make([]domain.Tuple, 0, expectedCount)

	for pos < len(data) {
		// Read field count (2 bytes, big-endian signed)
		if pos+2 > len(data) {
			return nil, fmt.Errorf("unexpected end of data reading field count")
		}
		fieldCount := int16(binary.BigEndian.Uint16(data[pos:]))
		pos += 2

		// Trailer: field_count = -1
		if fieldCount == -1 {
			break
		}

		if fieldCount != 6 {
			return nil, fmt.Errorf("expected 6 fields, got %d", fieldCount)
		}

		var t domain.Tuple

		// Field 0: object_type (SMALLINT = 2 bytes)
		if pos+4 > len(data) {
			return nil, fmt.Errorf("unexpected end reading object_type length")
		}
		length := int32(binary.BigEndian.Uint32(data[pos:]))
		pos += 4
		if length != 2 {
			return nil, fmt.Errorf("expected SMALLINT length 2, got %d", length)
		}
		t.ObjectType = domain.TypeID(int16(binary.BigEndian.Uint16(data[pos:])))
		pos += 2

		// Field 1: object_id (BIGINT = 8 bytes)
		if pos+4 > len(data) {
			return nil, fmt.Errorf("unexpected end reading object_id length")
		}
		length = int32(binary.BigEndian.Uint32(data[pos:]))
		pos += 4
		if length != 8 {
			return nil, fmt.Errorf("expected BIGINT length 8, got %d", length)
		}
		t.ObjectID = domain.ID(int64(binary.BigEndian.Uint64(data[pos:])))
		pos += 8

		// Field 2: relation (SMALLINT = 2 bytes)
		if pos+4 > len(data) {
			return nil, fmt.Errorf("unexpected end reading relation length")
		}
		length = int32(binary.BigEndian.Uint32(data[pos:]))
		pos += 4
		if length != 2 {
			return nil, fmt.Errorf("expected SMALLINT length 2, got %d", length)
		}
		t.Relation = domain.RelationID(int16(binary.BigEndian.Uint16(data[pos:])))
		pos += 2

		// Field 3: subject_type (SMALLINT = 2 bytes)
		if pos+4 > len(data) {
			return nil, fmt.Errorf("unexpected end reading subject_type length")
		}
		length = int32(binary.BigEndian.Uint32(data[pos:]))
		pos += 4
		if length != 2 {
			return nil, fmt.Errorf("expected SMALLINT length 2, got %d", length)
		}
		t.SubjectType = domain.TypeID(int16(binary.BigEndian.Uint16(data[pos:])))
		pos += 2

		// Field 4: subject_id (BIGINT = 8 bytes)
		if pos+4 > len(data) {
			return nil, fmt.Errorf("unexpected end reading subject_id length")
		}
		length = int32(binary.BigEndian.Uint32(data[pos:]))
		pos += 4
		if length != 8 {
			return nil, fmt.Errorf("expected BIGINT length 8, got %d", length)
		}
		t.SubjectID = domain.ID(int64(binary.BigEndian.Uint64(data[pos:])))
		pos += 8

		// Field 5: subject_relation (SMALLINT = 2 bytes)
		if pos+4 > len(data) {
			return nil, fmt.Errorf("unexpected end reading subject_relation length")
		}
		length = int32(binary.BigEndian.Uint32(data[pos:]))
		pos += 4
		if length != 2 {
			return nil, fmt.Errorf("expected SMALLINT length 2, got %d", length)
		}
		t.SubjectRelation = domain.RelationID(int16(binary.BigEndian.Uint16(data[pos:])))
		pos += 2

		tuples = append(tuples, t)
	}

	return tuples, nil
}
