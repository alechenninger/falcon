package postgres_test

// XID-range partitions are physical storage buckets. Snapshot cuts still decide
// membership, and the global sequence still orders the journal within a cut.
import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type snapshotPartitionLeaf struct {
	name    string
	lo, hi  uint64
	dropped bool
}
type snapshotPartitionEpoch struct {
	cut       snapshotFetchCut
	remaining int
}
type snapshotPartitionResult struct {
	InitialLeaves, FinalLeaves, PeakLeaves, Created, Dropped int
	SQL, Checkpoints, ReclaimedBytes                         int64
	DurableEpoch                                             int64
	DurableXmin                                              uint64
	Create, Detach, Drop, Checkpoint                         domainCaptureSummary
}
type snapshotPartitionManager struct {
	mu                               sync.Mutex
	c                                domainCaptureConfig
	leaves                           []snapshotPartitionLeaf
	end                              uint64
	epochs                           map[int64]*snapshotPartitionEpoch
	applied                          snapshotFetchCut
	durable                          snapshotFetchCut
	stats                            snapshotPartitionResult
	create, detach, drop, checkpoint []time.Duration
	measuring                        bool
}

func newSnapshotPartitionManager(c domainCaptureConfig) *snapshotPartitionManager {
	return &snapshotPartitionManager{c: c, epochs: map[int64]*snapshotPartitionEpoch{}}
}
func snapshotPartitionXid(ctx context.Context, db *pgxpool.Pool) (uint64, error) {
	var text string
	if err := db.QueryRow(ctx, "SELECT pg_current_xact_id()::text").Scan(&text); err != nil {
		return 0, err
	}
	return strconv.ParseUint(text, 10, 64)
}
func snapshotPartitionXmin(s snapshotBenchCut) (uint64, error) {
	first, _, ok := strings.Cut(s.text, ":")
	if !ok {
		return 0, fmt.Errorf("invalid partition checkpoint snapshot %q", s.text)
	}
	return strconv.ParseUint(first, 10, 64)
}

// Empty tables are prepared separately and attached using a matching CHECK.
// This uses the reduced parent lock of ATTACH instead of CREATE ... PARTITION OF.
func (m *snapshotPartitionManager) add(ctx context.Context, db *pgxpool.Pool, o replicationBenchObjects, lo, hi uint64) error {
	m.mu.Lock()
	index, measured := len(m.leaves), m.measuring
	m.mu.Unlock()
	name := fmt.Sprintf("%s_p%d", o.journal, index)
	start := time.Now()
	if _, err := db.Exec(ctx, "CREATE TABLE "+name+" (LIKE "+o.journal+" INCLUDING ALL)"); err != nil {
		return err
	}
	if _, err := db.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ADD CHECK(writer_xid >= '%d'::xid8 AND writer_xid < '%d'::xid8)", name, lo, hi)); err != nil {
		return err
	}
	if _, err := db.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION %s FOR VALUES FROM ('%d') TO ('%d')", o.journal, name, lo, hi)); err != nil {
		return err
	}
	m.mu.Lock()
	m.leaves = append(m.leaves, snapshotPartitionLeaf{name: name, lo: lo, hi: hi})
	m.end = hi
	if measured {
		m.stats.Created++
		m.stats.SQL += 3
		m.create = append(m.create, time.Since(start))
	}
	attached := 0
	for _, leaf := range m.leaves {
		if !leaf.dropped {
			attached++
		}
	}
	if attached > m.stats.PeakLeaves {
		m.stats.PeakLeaves = attached
	}
	m.mu.Unlock()
	return nil
}

func prepareSnapshotPartitionFixture(ctx context.Context, db *pgxpool.Pool, o replicationBenchObjects, c domainCaptureConfig) error {
	key := "PRIMARY KEY(position)"
	secondary := "writer_xid,position"
	partition := ""
	if c.CompoundKey || c.PartitionSpan > 0 {
		key, secondary = "PRIMARY KEY(writer_xid,position)", "position"
	}
	if c.PartitionSpan > 0 {
		partition = " PARTITION BY RANGE(writer_xid)"
	}
	if _, err := db.Exec(ctx, fmt.Sprintf(`CREATE SEQUENCE %s CACHE 1;
CREATE TABLE %s(position BIGINT NOT NULL DEFAULT nextval('%s'),root_id INT NOT NULL,
writer_xid XID8 NOT NULL DEFAULT pg_current_xact_id(),batch_id BIGINT NOT NULL,payload BYTEA NOT NULL,%s)%s;
ALTER SEQUENCE %s OWNED BY %s.position; ALTER TABLE %s ALTER COLUMN payload SET STORAGE EXTERNAL;
CREATE INDEX ON %s(%s); CREATE INDEX ON %s(root_id,writer_xid,position);
CREATE TABLE %s(epoch BIGINT PRIMARY KEY,previous_snapshot PG_SNAPSHOT NOT NULL,snapshot PG_SNAPSHOT NOT NULL);`,
		o.sequence, o.journal, o.sequence, key, partition, o.sequence, o.journal, o.journal, o.journal, secondary, o.journal, o.clock)); err != nil {
		return err
	}
	if c.PositionCover {
		return fmt.Errorf("partition fixture does not combine covering-index treatment")
	}
	base, err := snapshotPartitionXid(ctx, db)
	if err != nil {
		return err
	}
	base++
	historyTx := c.HistoryTx
	if historyTx <= 0 {
		historyTx = 1
	}
	boundary := base
	if c.History > 0 {
		// Real historical transactions span multiple older partitions. A shared
		// XID advance after loading keeps history out of the first live bucket.
		width := uint64(max(128, (historyTx+512+15)/16))
		boundary = base + 16*width
		if c.partitions != nil {
			for i := uint64(0); i < 16; i++ {
				lo := base + i*width
				if i == 0 {
					lo = 0
				}
				if err = c.partitions.add(ctx, db, o, lo, base+(i+1)*width); err != nil {
					return err
				}
			}
		}
	}
	if c.partitions != nil {
		ahead := c.PartitionAhead
		if ahead <= 0 {
			ahead = 32
		}
		for i := 0; i < ahead; i++ {
			lo := boundary + uint64(i)*uint64(c.PartitionSpan)
			if err = c.partitions.add(ctx, db, o, lo, lo+uint64(c.PartitionSpan)); err != nil {
				return err
			}
		}
	}
	if c.History > 0 {
		chunk := (c.History + historyTx - 1) / historyTx
		for first := 1; first <= c.History; first += chunk {
			if _, err = db.Exec(ctx, "INSERT INTO "+o.journal+"(root_id,batch_id,payload) SELECT n%$2,0,decode(repeat('ab',$3),'hex') FROM generate_series($1::int,$4::int)n", first, c.roots(), c.Rows*c.Payload, min(first+chunk-1, c.History)); err != nil {
				return err
			}
		}
		for {
			xid, e := snapshotPartitionXid(ctx, db)
			if e != nil {
				return e
			}
			if xid >= boundary {
				break
			}
		}
		// Analyze all leaves and the parent explicitly, equally before source
		// load; no explicit vacuum or JIT/parallel override is used.
		if c.partitions != nil {
			for _, leaf := range c.partitions.leaves {
				if _, err = db.Exec(ctx, "ANALYZE "+leaf.name); err != nil {
					return err
				}
			}
		}
		if _, err = db.Exec(ctx, "ANALYZE "+o.journal); err != nil {
			return err
		}
	}
	if c.PartitionGC {
		if c.partitions == nil {
			return fmt.Errorf("partition GC requires XID partitions")
		}
		if _, err = db.Exec(ctx, "CREATE TABLE "+o.clock+"_progress(id INT PRIMARY KEY,epoch BIGINT NOT NULL,snapshot PG_SNAPSHOT NOT NULL)"); err != nil {
			return err
		}
	}
	if _, err = db.Exec(ctx, "INSERT INTO "+o.clock+" VALUES(0,pg_current_snapshot(),pg_current_snapshot())"); err != nil {
		return err
	}
	if c.partitions != nil {
		c.partitions.stats.InitialLeaves = len(c.partitions.leaves)
	}
	return nil
}

func (m *snapshotPartitionManager) seal(cut snapshotFetchCut, roots int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.epochs[cut.epoch] = &snapshotPartitionEpoch{cut: cut, remaining: roots}
	m.advance()
}
func (m *snapshotPartitionManager) rootReady(epoch int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	x := m.epochs[epoch]
	if x == nil || x.remaining <= 0 {
		panic("partition root completion without registered fetch EOF")
	}
	x.remaining--
	m.advance()
}
func (m *snapshotPartitionManager) advance() {
	for {
		x := m.epochs[m.applied.epoch+1]
		if x == nil || x.remaining != 0 {
			return
		}
		m.applied = x.cut
		delete(m.epochs, x.cut.epoch)
	}
}
func (m *snapshotPartitionManager) beginMeasurement() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.measuring = true
}

func (m *snapshotPartitionManager) maintain(ctx context.Context, db *pgxpool.Pool, o replicationBenchObjects) error {
	// Allocation observes the current global XID even if older transactions
	// remain unresolved. It occurs only in the background manager.
	xid, err := snapshotPartitionXid(ctx, db)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.stats.SQL++
	end := m.end
	m.mu.Unlock()
	for end <= xid+uint64(2*m.c.PartitionSpan) {
		if err = m.add(ctx, db, o, end, end+uint64(m.c.PartitionSpan)); err != nil {
			return err
		}
		end += uint64(m.c.PartitionSpan)
	}
	m.mu.Lock()
	applied, durable := m.applied, m.durable
	m.mu.Unlock()
	if applied.epoch > durable.epoch {
		start := time.Now()
		if _, err = db.Exec(ctx, "INSERT INTO "+o.clock+"_progress VALUES(0,$1,$2::text::pg_snapshot) ON CONFLICT(id) DO UPDATE SET epoch=EXCLUDED.epoch,snapshot=EXCLUDED.snapshot", applied.epoch, applied.z.text); err != nil {
			return err
		}
		m.mu.Lock()
		m.durable = applied
		m.stats.Checkpoints++
		m.stats.SQL++
		m.checkpoint = append(m.checkpoint, time.Since(start))
		m.mu.Unlock()
		durable = applied
	}
	if durable.epoch == 0 {
		return nil
	}
	xmin, err := snapshotPartitionXmin(durable.z)
	if err != nil {
		return err
	}
	// The floor is derived from a completely applied, durably acknowledged cut.
	// A still-active old XID, or any lagging root, prevents unsafe retirement.
	for i := 0; ; i++ {
		m.mu.Lock()
		if i == len(m.leaves) {
			m.mu.Unlock()
			break
		}
		leaf := m.leaves[i]
		m.mu.Unlock()
		if leaf.dropped || leaf.hi > xmin {
			continue
		}
		var bytes int64
		if err = db.QueryRow(ctx, "SELECT pg_total_relation_size($1::regclass)", leaf.name).Scan(&bytes); err != nil {
			return err
		}
		start := time.Now()
		if _, err = db.Exec(ctx, "ALTER TABLE "+o.journal+" DETACH PARTITION "+leaf.name+" CONCURRENTLY"); err != nil {
			return err
		}
		detachTime := time.Since(start)
		start = time.Now()
		if _, err = db.Exec(ctx, "DROP TABLE "+leaf.name); err != nil {
			return err
		}
		m.mu.Lock()
		m.leaves[i].dropped = true
		m.stats.Dropped++
		m.stats.SQL += 3
		m.stats.ReclaimedBytes += bytes
		m.detach = append(m.detach, detachTime)
		m.drop = append(m.drop, time.Since(start))
		m.mu.Unlock()
	}
	return nil
}
func (m *snapshotPartitionManager) start(ctx context.Context, url string, o replicationBenchObjects, fail func(error), wg *sync.WaitGroup) error {
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return err
	}
	config.MaxConns = 1
	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return err
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer db.Close()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			if err := m.maintain(ctx, db, o); err != nil {
				if ctx.Err() == nil {
					fail(err)
				}
				return
			}
		}
	}()
	return nil
}
func (m *snapshotPartitionManager) result() *snapshotPartitionResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.stats
	for _, leaf := range m.leaves {
		if !leaf.dropped {
			r.FinalLeaves++
		}
	}
	r.DurableEpoch = m.durable.epoch
	if m.durable.epoch > 0 {
		r.DurableXmin, _ = snapshotPartitionXmin(m.durable.z)
	}
	r.Create, r.Detach, r.Drop, r.Checkpoint = summarizeDomainDurations(m.create), summarizeDomainDurations(m.detach), summarizeDomainDurations(m.drop), summarizeDomainDurations(m.checkpoint)
	return &r
}

func BenchmarkSnapshotPartitions(b *testing.B) {
	c := domainCaptureConfig{Name: "partitions", Mode: "snapshot-routed", SnapshotFetch: "pipeline-stream", SnapshotWake: "notify", SnapshotProbe: "indexed", FetchPlan: "guarded"}.defaults()
	if err := loadSnapshotFetchConfig(&c); err != nil {
		b.Fatal(err)
	}
	runDomainCapture(b, c.defaults())
}
