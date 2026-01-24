package grpc

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	"github.com/alechenninger/falcon/internal/application/observability"
	"github.com/alechenninger/falcon/internal/domain"
	infragrpc "github.com/alechenninger/falcon/internal/infrastructure/grpc"
	graphpb "github.com/alechenninger/falcon/internal/infrastructure/grpc/proto"
)

// Config holds the configuration for a falcon server node.
type Config struct {
	// ShardID is the unique identifier for this shard (e.g., "shard-0").
	ShardID domain.ShardID

	// ListenAddr is the address to listen on (e.g., ":50051").
	ListenAddr string

	// Peers is a map of shard ID to gRPC address (e.g., {"shard-1": "localhost:50052"}).
	// Should NOT include this shard.
	Peers map[domain.ShardID]string

	// NumShards is the total number of shards in the cluster.
	// Used for the router function.
	NumShards int

	// TestDataConfig configures the generated test data.
	TestDataConfig domain.TestDataConfig

	// Verbose enables debug-level logging.
	Verbose bool
}

// ParsePeers parses a comma-separated list of "shardID=addr" pairs.
// Example: "shard-1=localhost:50052,shard-2=localhost:50053"
func ParsePeers(s string) (map[domain.ShardID]string, error) {
	if s == "" {
		return nil, nil
	}

	peers := make(map[domain.ShardID]string)
	for _, pair := range strings.Split(s, ",") {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid peer format: %q (expected shardID=addr)", pair)
		}
		shardID := domain.ShardID(strings.TrimSpace(parts[0]))
		addr := strings.TrimSpace(parts[1])
		peers[shardID] = addr
	}
	return peers, nil
}

// Runner represents a running falcon gRPC server.
type Runner struct {
	config     Config
	grpcServer *grpc.Server
	graph      *domain.ShardedGraph
	listener   net.Listener
	conns      []*grpc.ClientConn
}

// Start creates and starts a falcon server with the given configuration.
// It generates deterministic test data, filters to this shard, connects to peers,
// and starts serving gRPC requests.
func Start(ctx context.Context, cfg Config) (*Runner, error) {
	log.Printf("[%s] Starting falcon server on %s", cfg.ShardID, cfg.ListenAddr)

	// Create schema first (needed for test data generation)
	sch := domain.TestDataSchema()
	router := domain.TestDataRouter(cfg.NumShards, sch)

	// Generate test data
	log.Printf("[%s] Generating test data...", cfg.ShardID)
	startGen := time.Now()
	allTuples := domain.GenerateTestData(cfg.TestDataConfig, sch)
	log.Printf("[%s] Generated %d tuples in %v", cfg.ShardID, len(allTuples), time.Since(startGen))

	// Create static store with ALL tuples (store is not sharded)
	// ShardedGraph.Start() will filter during hydration via filteringTupleIterator
	staticStore := newStaticStore(allTuples)

	// Create slog logger with shard ID and appropriate log level
	logLevel := slog.LevelInfo
	if cfg.Verbose {
		logLevel = slog.LevelDebug
	}
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	logger := slog.New(handler).With("shard", cfg.ShardID)

	shardedGraph := domain.NewShardedGraph(
		cfg.ShardID,
		sch,
		router,
		nil, // Remote shards added later
		staticStore,
		staticStore,
	).
		WithUsersetsObserver(observability.NewUsersetsObserver(logger)).
		WithGraphObserver(observability.NewShardedGraphObserver(logger)).
		WithCheckObserver(observability.NewCheckObserver(logger))

	// Start the graph (hydrate from static store)
	log.Printf("[%s] Hydrating usersets...", cfg.ShardID)
	startHydrate := time.Now()

	// Run Start in a goroutine since it blocks on Subscribe
	// For static store, Subscribe returns immediately
	errCh := make(chan error, 1)
	go func() {
		errCh <- shardedGraph.Start(ctx)
	}()

	// Wait a bit for hydration to complete
	select {
	case err := <-errCh:
		if err != nil {
			return nil, fmt.Errorf("failed to start graph: %w", err)
		}
	case <-time.After(10 * time.Minute):
		return nil, fmt.Errorf("timeout waiting for hydration")
	}

	// Set replicated time to the static store's current time
	// This is needed because the static store doesn't emit changes to advance the time
	currentTime, _ := staticStore.CurrentTime(ctx)
	shardedGraph.SetReplicatedTime(currentTime)
	log.Printf("[%s] Hydration complete in %v (replicatedTime=%d)", cfg.ShardID, time.Since(startHydrate), currentTime)

	// Clear the static store to free memory - tuples are now in the graph
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	staticStore.Clear()
	runtime.GC()
	runtime.GC() // Run twice to ensure finalizers complete
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	freedMB := float64(memBefore.HeapAlloc-memAfter.HeapAlloc) / (1024 * 1024)
	log.Printf("[%s] Cleared static store, freed %.1f MB", cfg.ShardID, freedMB)

	// Connect to peer shards
	var conns []*grpc.ClientConn
	for peerID, addr := range cfg.Peers {
		log.Printf("[%s] Connecting to peer %s at %s", cfg.ShardID, peerID, addr)
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			// Clean up any connections we already made
			for _, c := range conns {
				c.Close()
			}
			return nil, fmt.Errorf("failed to connect to peer %s: %w", peerID, err)
		}
		conns = append(conns, conn)

		client := graphpb.NewGraphServiceClient(conn)
		remoteGraph := infragrpc.NewRemoteGraph(client, sch)
		shardedGraph.SetRemoteShard(peerID, remoteGraph)
	}

	// Create and start gRPC server
	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		for _, c := range conns {
			c.Close()
		}
		return nil, fmt.Errorf("failed to listen on %s: %w", cfg.ListenAddr, err)
	}

	grpcServer := grpc.NewServer()
	graphServer := NewServer(shardedGraph)
	graphpb.RegisterGraphServiceServer(grpcServer, graphServer)
	reflection.Register(grpcServer) // Enable reflection for grpcurl

	log.Printf("[%s] Server ready, listening on %s", cfg.ShardID, cfg.ListenAddr)

	return &Runner{
		config:     cfg,
		grpcServer: grpcServer,
		graph:      shardedGraph,
		listener:   listener,
		conns:      conns,
	}, nil
}

// Serve starts serving requests. This blocks until the server is stopped.
func (r *Runner) Serve() error {
	return r.grpcServer.Serve(r.listener)
}

// Stop gracefully stops the server.
func (r *Runner) Stop() {
	log.Printf("[%s] Stopping server...", r.config.ShardID)
	r.grpcServer.GracefulStop()
	for _, c := range r.conns {
		c.Close()
	}
	log.Printf("[%s] Server stopped", r.config.ShardID)
}

// Run starts the server and blocks until interrupted (SIGINT/SIGTERM).
func Run(ctx context.Context, cfg Config) error {
	runner, err := Start(ctx, cfg)
	if err != nil {
		return err
	}

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Serve()
	}()

	select {
	case sig := <-sigCh:
		log.Printf("[%s] Received signal %v, shutting down", cfg.ShardID, sig)
		runner.Stop()
		return nil
	case err := <-errCh:
		return err
	case <-ctx.Done():
		runner.Stop()
		return ctx.Err()
	}
}

// staticStore is a read-only store backed by a pre-generated slice of tuples.
// It implements both domain.Store and domain.ChangeStream.
type staticStore struct {
	tuples      []domain.Tuple
	currentTime domain.StoreTime
}

func newStaticStore(tuples []domain.Tuple) *staticStore {
	// Use length as the "current time" - each tuple is a unique point in time
	return &staticStore{
		tuples:      tuples,
		currentTime: domain.StoreTime(len(tuples)),
	}
}

// Begin returns a read-only transaction that does not support writes.
func (s *staticStore) Begin(ctx context.Context) (domain.Tx, error) {
	return &staticTx{}, nil
}

// LoadAll returns an iterator over all tuples.
func (s *staticStore) LoadAll(ctx context.Context) (domain.TupleIterator, error) {
	return domain.NewSliceIterator(s.tuples), nil
}

// Close is a no-op.
func (s *staticStore) Close() error {
	return nil
}

// Clear releases the tuples slice to free memory after hydration.
// The store remains valid for CurrentTime() calls but LoadAll() will return empty.
func (s *staticStore) Clear() {
	s.tuples = nil
}

// Subscribe returns channels that complete immediately (no live updates).
func (s *staticStore) Subscribe(ctx context.Context, after domain.StoreTime) (<-chan domain.Change, <-chan error) {
	changes := make(chan domain.Change)
	errCh := make(chan error, 1)

	// Close immediately - no updates for static data
	close(changes)

	return changes, errCh
}

// CurrentTime returns the static current time.
func (s *staticStore) CurrentTime(ctx context.Context) (domain.StoreTime, error) {
	return s.currentTime, nil
}

// staticTx is a read-only transaction that does not support writes.
type staticTx struct{}

func (t *staticTx) GetID(ctx context.Context, ref domain.ObjectRef) (domain.ID, error) {
	return 0, domain.ErrIDNotFound
}

func (t *staticTx) GetOrProvisionID(ctx context.Context, ref domain.ObjectRef, root domain.ObjectRef) (domain.ID, error) {
	return 0, fmt.Errorf("static store does not support ID provisioning")
}

func (t *staticTx) GetRef(ctx context.Context, typeID domain.TypeID, id domain.ID) (domain.ObjectRef, error) {
	return domain.ObjectRef{}, domain.ErrIDNotFound
}

func (t *staticTx) Write(ctx context.Context, mutations []domain.Mutation) error {
	return fmt.Errorf("static store does not support writes")
}

func (t *staticTx) Contains(ctx context.Context, predicate domain.TuplePredicate) (bool, error) {
	return false, fmt.Errorf("static store does not support contains check")
}

func (t *staticTx) Commit(ctx context.Context) error {
	return nil
}

func (t *staticTx) Rollback(ctx context.Context) error {
	return nil
}

// Compile-time interface checks
var (
	_ domain.Store        = (*staticStore)(nil)
	_ domain.ChangeStream = (*staticStore)(nil)
	_ domain.Tx           = (*staticTx)(nil)
)
