package postgres

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/alechenninger/falcon/internal/domain"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Stream implements domain.ChangeStream using PostgreSQL logical replication.
// It uses the pgoutput plugin via the pglogrepl library.
type Stream struct {
	connString  string
	slotName    string
	publication string
}

// NewStream creates a new Stream.
// The connString should be a PostgreSQL connection string.
// slotName is the name of the replication slot to create/use.
// publication is the name of the publication to subscribe to.
func NewStream(connString, slotName, publication string) *Stream {
	return &Stream{
		connString:  connString,
		slotName:    slotName,
		publication: publication,
	}
}

// Subscribe implements domain.ChangeStream.Subscribe.
// It connects to the replication stream and emits changes.
func (s *Stream) Subscribe(ctx context.Context, after domain.StoreTime) (<-chan domain.Change, <-chan error) {
	changes := make(chan domain.Change, 100)
	errCh := make(chan error, 1)

	go func() {
		defer close(changes)
		defer close(errCh)

		if err := s.stream(ctx, after, changes); err != nil {
			errCh <- err
		}
	}()

	return changes, errCh
}

// CurrentTime returns the current WAL flush position.
func (s *Stream) CurrentTime(ctx context.Context) (domain.StoreTime, error) {
	conn, err := pgconn.Connect(ctx, s.connString)
	if err != nil {
		return 0, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close(ctx)

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return 0, fmt.Errorf("failed to identify system: %w", err)
	}

	return domain.StoreTime(sysident.XLogPos), nil
}

// EnsureReplicationSetup creates the publication and replication slot if they don't exist.
// This should be called once during setup, using a non-replication connection.
func (s *Stream) EnsureReplicationSetup(ctx context.Context, conn *pgconn.PgConn) error {
	// Create publication if it doesn't exist
	_, err := conn.Exec(ctx, fmt.Sprintf(
		"CREATE PUBLICATION %s FOR TABLE tuples",
		pgQuoteIdent(s.publication),
	)).ReadAll()
	if err != nil {
		// Ignore "already exists" errors
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("failed to create publication: %w", err)
		}
	}

	return nil
}

// EnsureReplicationSlot creates the replication slot if it doesn't exist.
// This requires a replication connection.
func (s *Stream) EnsureReplicationSlot(ctx context.Context, conn *pgconn.PgConn) error {
	_, err := pglogrepl.CreateReplicationSlot(ctx, conn, s.slotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Temporary: false,
		})
	if err != nil {
		// Ignore "already exists" errors
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("failed to create replication slot: %w", err)
		}
	}
	return nil
}

func (s *Stream) stream(ctx context.Context, after domain.StoreTime, changes chan<- domain.Change) error {
	// Connect with replication mode
	connString := s.connString
	if !strings.Contains(connString, "replication=") {
		if strings.Contains(connString, "?") {
			connString += "&replication=database"
		} else {
			connString += "?replication=database"
		}
	}

	conn, err := pgconn.Connect(ctx, connString)
	if err != nil {
		return fmt.Errorf("failed to connect for replication: %w", err)
	}
	defer conn.Close(ctx)

	// Ensure slot exists
	if err := s.EnsureReplicationSlot(ctx, conn); err != nil {
		return err
	}

	// Start replication
	startLSN := pglogrepl.LSN(after)
	err = pglogrepl.StartReplication(ctx, conn, s.slotName, startLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", s.publication),
			},
		})
	if err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	// Process replication messages
	standbyDeadline := time.Now().Add(10 * time.Second)
	var clientXLogPos pglogrepl.LSN
	var relations = make(map[uint32]*pglogrepl.RelationMessage)

	for {
		if time.Now().After(standbyDeadline) {
			// Send standby status update
			err = pglogrepl.SendStandbyStatusUpdate(ctx, conn,
				pglogrepl.StandbyStatusUpdate{
					WALWritePosition: clientXLogPos,
				})
			if err != nil {
				return fmt.Errorf("failed to send standby status: %w", err)
			}
			standbyDeadline = time.Now().Add(10 * time.Second)
		}

		ctxDeadline, cancel := context.WithDeadline(ctx, standbyDeadline)
		rawMsg, err := conn.ReceiveMessage(ctxDeadline)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("failed to receive message: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("received error: %s", errMsg.Message)
		}

		copyData, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("failed to parse keepalive: %w", err)
			}
			if pkm.ReplyRequested {
				err = pglogrepl.SendStandbyStatusUpdate(ctx, conn,
					pglogrepl.StandbyStatusUpdate{
						WALWritePosition: clientXLogPos,
					})
				if err != nil {
					return fmt.Errorf("failed to send standby status: %w", err)
				}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("failed to parse xlog data: %w", err)
			}

			msg, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				return fmt.Errorf("failed to parse logical replication message: %w", err)
			}

			change := s.processMessage(msg, relations, uint64(xld.WALStart))
			if change != nil {
				select {
				case changes <- *change:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
		}
	}
}

func (s *Stream) processMessage(msg pglogrepl.Message, relations map[uint32]*pglogrepl.RelationMessage, lsn uint64) *domain.Change {
	switch m := msg.(type) {
	case *pglogrepl.RelationMessage:
		relations[m.RelationID] = m
		return nil

	case *pglogrepl.InsertMessage:
		rel, ok := relations[m.RelationID]
		if !ok {
			return nil
		}
		tuple, err := s.decodeTuple(rel, m.Tuple)
		if err != nil {
			return nil
		}
		return &domain.Change{
			Time:  domain.StoreTime(lsn),
			Op:    domain.OpInsert,
			Tuple: *tuple,
		}

	case *pglogrepl.UpdateMessage:
		rel, ok := relations[m.RelationID]
		if !ok {
			return nil
		}
		tuple, err := s.decodeTuple(rel, m.NewTuple)
		if err != nil {
			return nil
		}
		// Treat updates as inserts
		return &domain.Change{
			Time:  domain.StoreTime(lsn),
			Op:    domain.OpInsert,
			Tuple: *tuple,
		}

	case *pglogrepl.DeleteMessage:
		rel, ok := relations[m.RelationID]
		if !ok {
			return nil
		}
		tuple, err := s.decodeTuple(rel, m.OldTuple)
		if err != nil {
			return nil
		}
		return &domain.Change{
			Time:  domain.StoreTime(lsn),
			Op:    domain.OpDelete,
			Tuple: *tuple,
		}

	default:
		// Begin, Commit, Origin, Type messages - ignore
		return nil
	}
}

func (s *Stream) decodeTuple(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) (*domain.Tuple, error) {
	if tuple == nil {
		return nil, fmt.Errorf("nil tuple data")
	}

	values := make(map[string]string)
	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}
		colName := rel.Columns[i].Name
		switch col.DataType {
		case 't': // text (integers are sent as text in pgoutput)
			values[colName] = string(col.Data)
		case 'n': // null
			values[colName] = "0"
		}
	}

	objectType, err := parseSmallInt(values["object_type"])
	if err != nil {
		return nil, fmt.Errorf("failed to parse object_type: %w", err)
	}
	objectID, err := parseOID(values["object_id"])
	if err != nil {
		return nil, fmt.Errorf("failed to parse object_id: %w", err)
	}
	relation, err := parseSmallInt(values["relation"])
	if err != nil {
		return nil, fmt.Errorf("failed to parse relation: %w", err)
	}
	subjectType, err := parseSmallInt(values["subject_type"])
	if err != nil {
		return nil, fmt.Errorf("failed to parse subject_type: %w", err)
	}
	subjectID, err := parseOID(values["subject_id"])
	if err != nil {
		return nil, fmt.Errorf("failed to parse subject_id: %w", err)
	}
	subjectRelation, err := parseSmallInt(values["subject_relation"])
	if err != nil {
		return nil, fmt.Errorf("failed to parse subject_relation: %w", err)
	}

	return &domain.Tuple{
		ObjectType:      domain.TypeID(objectType),
		ObjectID:        domain.ID(objectID),
		Relation:        domain.RelationID(relation),
		SubjectType:     domain.TypeID(subjectType),
		SubjectID:       domain.ID(subjectID),
		SubjectRelation: domain.RelationID(subjectRelation),
	}, nil
}

func parseOID(s string) (uint32, error) {
	v, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(v), nil
}

func parseSmallInt(s string) (int16, error) {
	v, err := strconv.ParseInt(s, 10, 16)
	if err != nil {
		return 0, err
	}
	return int16(v), nil
}

func pgQuoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// ParseLSN parses a PostgreSQL LSN string (e.g., "0/16B3748") into a StoreTime.
func ParseLSN(s string) (domain.StoreTime, error) {
	lsn, err := pglogrepl.ParseLSN(s)
	if err != nil {
		return 0, err
	}
	return domain.StoreTime(lsn), nil
}

// FormatLSN formats a StoreTime as a PostgreSQL LSN string (e.g., "0/16B3748").
func FormatLSN(t domain.StoreTime) string {
	return pglogrepl.LSN(t).String()
}

// Compile-time interface check
var _ domain.ChangeStream = (*Stream)(nil)
