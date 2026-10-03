package postgres_test

import (
	"context"
	"fmt"
	"hash/crc32"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestDomainCaptureProtocols(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	connString, cleanup, _ := setupReplicationBenchPostgres(ctx, t)
	defer cleanup()
	p, err := pgxpool.New(ctx, connString)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for _, mode := range []string{"counter-global", "counter-tenant", "counter-root"} {
		t.Run(mode, func(t *testing.T) {
			c := domainCaptureConfig{Mode: mode, Tenants: 2, RootsPerTenant: 2}.defaults()
			o := newReplicationBenchObjects()
			if err := prepareDomainCapture(ctx, p, o, c); err != nil {
				t.Fatal(err)
			}
			appendTx := func(tx pgx.Tx, root int, id int64) int64 {
				var pos int64
				query := fmt.Sprintf(`WITH h AS(UPDATE %s SET position=position+1 WHERE domain=$1 RETURNING position) INSERT INTO %s(domain,position,root_id,batch_id,payload) SELECT $1,position,$2,$3,$4 FROM h RETURNING position`, o.clock, o.journal)
				if err := tx.QueryRow(ctx, query, c.domain(root), root, id, domainCapturePayload(id, c)).Scan(&pos); err != nil {
					t.Fatal(err)
				}
				return pos
			}
			first, err := p.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer first.Rollback(context.Background())
			if pos := appendTx(first, 0, 1); pos != 1 {
				t.Fatal(pos)
			}
			var head int64
			if err = p.QueryRow(ctx, "SELECT position FROM "+o.clock+" WHERE domain=$1", c.domain(0)).Scan(&head); err != nil || head != 0 {
				t.Fatalf("uncommitted head visible: %d %v", head, err)
			}
			other, err := p.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer other.Rollback(context.Background())
			_, err = other.Exec(ctx, "SET LOCAL lock_timeout='40ms'")
			if err != nil {
				t.Fatal(err)
			}
			root := 2 // another tenant; only the global counter should block it
			query := fmt.Sprintf(`WITH h AS(UPDATE %s SET position=position+1 WHERE domain=$1 RETURNING position) INSERT INTO %s(domain,position,root_id,batch_id,payload) SELECT $1,position,$2,$3,$4 FROM h RETURNING position`, o.clock, o.journal)
			var pos int64
			err = other.QueryRow(ctx, query, c.domain(root), root, int64(2), domainCapturePayload(2, c)).Scan(&pos)
			if mode == "counter-global" {
				if err == nil {
					t.Fatal("global counter failed to serialize")
				}
				other.Rollback(ctx)
			} else {
				if err != nil {
					t.Fatalf("unrelated domain blocked: %v", err)
				}
				if err = other.Commit(ctx); err != nil {
					t.Fatal(err)
				}
			}
			sibling, err := p.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer sibling.Rollback(context.Background())
			if _, err = sibling.Exec(ctx, "SET LOCAL lock_timeout='40ms'"); err != nil {
				t.Fatal(err)
			}
			err = sibling.QueryRow(ctx, query, c.domain(1), 1, int64(3), domainCapturePayload(3, c)).Scan(&pos)
			if mode == "counter-root" {
				if err != nil {
					t.Fatalf("sibling root blocked: %v", err)
				}
			} else if err == nil {
				t.Fatal("shared counter failed to serialize sibling")
			}
			sibling.Rollback(ctx)
			if err = first.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			retry, err := p.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if pos = appendTx(retry, 0, 1); pos != 1 {
				t.Fatalf("rollback left gap: %d", pos)
			}
			if err = retry.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			var count int
			if err = p.QueryRow(ctx, "SELECT count(*) FROM "+o.journal+" WHERE domain=$1", c.domain(0)).Scan(&count); err != nil || count != 1 {
				t.Fatalf("journal rollback incorrect: %d %v", count, err)
			}
		})
	}
	t.Run("counter-bounded-pagination", func(t *testing.T) {
		c := domainCaptureConfig{Mode: "counter-root", Tenants: 2, RootsPerTenant: 2, History: 550}.defaults()
		o := newReplicationBenchObjects()
		if err := prepareDomainCapture(ctx, p, o, c); err != nil {
			t.Fatal(err)
		}
		ds := []int32{0, 1, 2, 3}
		lo := []int64{0, 0, 0, 0}
		hi := make([]int64, 4)
		for i := range hi {
			if err := p.QueryRow(ctx, "SELECT position FROM "+o.clock+" WHERE domain=$1", i).Scan(&hi[i]); err != nil {
				t.Fatal(err)
			}
		}
		cursorD, cursorP := -1, int64(0)
		counts := make([]int64, 4)
		seen := map[[2]int64]bool{}
		for {
			rows, err := p.Query(ctx, domainCounterPageQuery(o, "bounded"), ds, lo, hi, cursorD, cursorP)
			if err != nil {
				t.Fatal(err)
			}
			n := 0
			for rows.Next() {
				var d, r int
				var pos, id int64
				var payload []byte
				if err = rows.Scan(&d, &pos, &r, &id, &payload); err != nil {
					rows.Close()
					t.Fatal(err)
				}
				key := [2]int64{int64(d), pos}
				if seen[key] || pos != counts[d]+1 || r != d {
					rows.Close()
					t.Fatalf("invalid closed prefix %v", key)
				}
				seen[key] = true
				counts[d]++
				cursorD, cursorP = d, pos
				n++
			}
			rows.Close()
			if err = rows.Err(); err != nil {
				t.Fatal(err)
			}
			if n < 128 {
				break
			}
		}
		if len(seen) != 550 {
			t.Fatalf("lost positions: %d", len(seen))
		}
		for i := range hi {
			if counts[i] != hi[i] {
				t.Fatalf("incomplete domain %d: %d/%d", i, counts[i], hi[i])
			}
		}
	})
	t.Run("snapshot-replay-pagination-late-commit", func(t *testing.T) {
		c := domainCaptureConfig{Mode: "snapshot-roots", Tenants: 1, RootsPerTenant: 2}.defaults()
		o := newReplicationBenchObjects()
		if err := prepareDomainCapture(ctx, p, o, c); err != nil {
			t.Fatal(err)
		}
		var initial string
		if err = p.QueryRow(ctx, "SELECT snapshot::text FROM "+o.clock+" WHERE epoch=0").Scan(&initial); err != nil {
			t.Fatal(err)
		}
		a, err := parseSnapshotBenchCut(initial)
		if err != nil {
			t.Fatal(err)
		}
		pending, err := p.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer pending.Rollback(context.Background())
		var xid string
		if err = pending.QueryRow(ctx, "SELECT pg_current_xact_id()::text").Scan(&xid); err != nil {
			t.Fatal(err)
		}
		_, err = p.Exec(ctx, fmt.Sprintf(`INSERT INTO %s(root_id,batch_id,payload) SELECT 1,n,decode(repeat('ab',256),'hex') FROM generate_series(1,260)n`, o.journal))
		if err != nil {
			t.Fatal(err)
		}
		aborted, err := p.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = aborted.Exec(ctx, "INSERT INTO "+o.journal+"(root_id,batch_id,payload) VALUES(0,999,'a')"); err != nil {
			t.Fatal(err)
		}
		if err = aborted.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		m := &domainCaptureMetrics{}
		z, err := captureDomainSnapshot(ctx, p, o, 1, a, m)
		if err != nil {
			t.Fatal(err)
		}
		// Manifest already durable before any payload read. Commit an old xid AFTER
		// this cut, then replay that durable cut: it must remain excluded.
		if _, err = pending.Exec(ctx, "INSERT INTO "+o.journal+"(root_id,batch_id,payload) VALUES(0,261,'b')"); err != nil {
			t.Fatal(err)
		}
		if err = pending.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		var previous, next string
		if err = p.QueryRow(ctx, "SELECT previous_snapshot::text,snapshot::text FROM "+o.clock+" WHERE epoch=1").Scan(&previous, &next); err != nil {
			t.Fatal(err)
		}
		a, err = parseSnapshotBenchCut(previous)
		if err != nil {
			t.Fatal(err)
		}
		z, err = parseSnapshotBenchCut(next)
		if err != nil {
			t.Fatal(err)
		}
		root := 1
		frames, err := domainSnapshotFrames(ctx, p, o, a, z, &root, m, nil)
		if err != nil || len(frames) != 260 {
			t.Fatalf("replay incomplete %d %v", len(frames), err)
		}
		for i, f := range frames {
			if f.id != int64(i+1) {
				t.Fatalf("replay ordering %d", f.id)
			}
		}
		root = 0
		frames, err = domainSnapshotFrames(ctx, p, o, a, z, &root, m, nil)
		if err != nil || len(frames) != 0 {
			t.Fatalf("excluded late/aborted write visible: %v %v", frames, err)
		}
		newer, err := captureDomainSnapshot(ctx, p, o, 2, z, m)
		if err != nil {
			t.Fatal(err)
		}
		frames, err = domainSnapshotFrames(ctx, p, o, z, newer, &root, m, nil)
		if err != nil || len(frames) != 1 || frames[0].id != 261 {
			t.Fatalf("old active xid lost: %v %v xid=%s", frames, err, xid)
		}
	})
}

func TestDomainCaptureSchedulerIndependentRoots(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s := newDomainCaptureScheduler()
	m := &domainCaptureMetrics{}
	for _, task := range []domainCaptureTask{{root: 0, count: 1}, {root: 0, count: 2}, {root: 1, count: 3}} {
		if err := s.enqueue(ctx, task, m); err != nil {
			t.Fatal(err)
		}
	}
	first, ok := s.take(ctx)
	if !ok || first.root != 0 || first.count != 1 {
		t.Fatal(first)
	}
	// Root 0 is still active. Its next task cannot overtake, but root 1 can run.
	second, ok := s.take(ctx)
	if !ok || second.root != 1 {
		t.Fatal(second)
	}
	s.complete(1)
	s.complete(0)
	third, ok := s.take(ctx)
	if !ok || third.root != 0 || third.count != 2 {
		t.Fatal(third)
	}
	s.complete(0)
	if len(s.slots) != 0 {
		t.Fatal("scheduler leaked task capacity")
	}
}
func TestDomainCaptureIntegrity(t *testing.T) {
	c := domainCaptureConfig{RootsPerTenant: 2}.defaults()
	payload := domainCapturePayload(1, c)
	m := &domainCaptureMetrics{events: make([]domainCaptureEvent, 2), latest: map[[2]int64]int64{}, done: make(chan struct{})}
	m.events[1] = domainCaptureEvent{root: 1, key: 3, crc: crc32.ChecksumIEEE(payload)}
	bad := domainCaptureFrame{root: 1, id: 1, payload: []byte("bad")}
	if err := m.apply(c, []domainCaptureFrame{bad}); err == nil {
		t.Fatal("corruption accepted")
	}
	good := domainCaptureFrame{root: 1, id: 1, payload: payload}
	if err := m.apply(c, []domainCaptureFrame{good}); err != nil {
		t.Fatal(err)
	}
	if err := m.apply(c, nil); err != nil {
		t.Fatal(err)
	}
	if err := m.apply(c, []domainCaptureFrame{good}); err == nil {
		t.Fatal("duplicate accepted")
	}
}
