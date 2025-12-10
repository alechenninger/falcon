// Package server provides a runnable falcon server for distributed testing.
package server

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	"github.com/alechenninger/falcon/graph"
	graphpb "github.com/alechenninger/falcon/graph/proto"
	"github.com/alechenninger/falcon/probes/logging"
	"github.com/alechenninger/falcon/store"
)

// Config holds the configuration for a falcon server node.
type Config struct {
	// ShardID is the unique identifier for this shard (e.g., "shard-0").
	ShardID graph.ShardID

	// ListenAddr is the address to listen on (e.g., ":50051").
	ListenAddr string

	// Peers is a map of shard ID to gRPC address (e.g., {"shard-1": "localhost:50052"}).
	// Should NOT include this shard.
	Peers map[graph.ShardID]string

	// NumShards is the total number of shards in the cluster.
	// Used for the router function.
	NumShards int

	// TestDataConfig configures the generated test data.
	TestDataConfig graph.TestDataConfig

	// Verbose enables debug-level logging.
	Verbose bool
}

// ParsePeers parses a comma-separated list of "shardID=addr" pairs.
// Example: "shard-1=localhost:50052,shard-2=localhost:50053"
func ParsePeers(s string) (map[graph.ShardID]string, error) {
	if s == "" {
		return nil, nil
	}

	peers := make(map[graph.ShardID]string)
	for _, pair := range strings.Split(s, ",") {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid peer format: %q (expected shardID=addr)", pair)
		}
		shardID := graph.ShardID(strings.TrimSpace(parts[0]))
		addr := strings.TrimSpace(parts[1])
		peers[shardID] = addr
	}
	return peers, nil
}

// Server represents a running falcon server.
type Server struct {
	config     Config
	grpcServer *grpc.Server
	graph      *graph.ShardedGraph
	listener   net.Listener
	conns      []*grpc.ClientConn
}

// Start creates and starts a falcon server with the given configuration.
// It generates deterministic test data, filters to this shard, connects to peers,
// and starts serving gRPC requests.
func Start(ctx context.Context, cfg Config) (*Server, error) {
	log.Printf("[%s] Starting falcon server on %s", cfg.ShardID, cfg.ListenAddr)

	// Generate test data
	log.Printf("[%s] Generating test data...", cfg.ShardID)
	startGen := time.Now()
	allTuples := graph.GenerateTestData(cfg.TestDataConfig)
	log.Printf("[%s] Generated %d tuples in %v", cfg.ShardID, len(allTuples), time.Since(startGen))

	// Create static store with ALL tuples (store is not sharded)
	// ShardedGraph.Start() will filter during hydration via filteringTupleIterator
	router := graph.TestDataRouter(cfg.NumShards)
	staticStore := newStaticStore(allTuples)

	// Create the sharded graph (initially without remote shards - we'll add them after)
	sch := graph.TestDataSchema()

	// Create slog logger with shard ID and appropriate log level
	logLevel := slog.LevelInfo
	if cfg.Verbose {
		logLevel = slog.LevelDebug
	}
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	logger := slog.New(handler).With("shard", cfg.ShardID)

	shardedGraph := graph.NewShardedGraph(
		cfg.ShardID,
		sch,
		router,
		nil, // Remote shards added later
		staticStore,
		staticStore,
	).
		WithUsersetsObserver(logging.NewUsersetsObserver(logger)).
		WithGraphObserver(logging.NewShardedGraphObserver(logger)).
		WithCheckObserver(logging.NewCheckObserver(logger))

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
		remoteGraph := graph.NewRemoteGraph(client, sch)
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
	graphServer := graph.NewGraphServer(shardedGraph)
	graphpb.RegisterGraphServiceServer(grpcServer, graphServer)
	reflection.Register(grpcServer) // Enable reflection for grpcurl

	log.Printf("[%s] Server ready, listening on %s", cfg.ShardID, cfg.ListenAddr)

	return &Server{
		config:     cfg,
		grpcServer: grpcServer,
		graph:      shardedGraph,
		listener:   listener,
		conns:      conns,
	}, nil
}

// Serve starts serving requests. This blocks until the server is stopped.
func (s *Server) Serve() error {
	return s.grpcServer.Serve(s.listener)
}

// Stop gracefully stops the server.
func (s *Server) Stop() {
	log.Printf("[%s] Stopping server...", s.config.ShardID)
	s.grpcServer.GracefulStop()
	for _, c := range s.conns {
		c.Close()
	}
	log.Printf("[%s] Server stopped", s.config.ShardID)
}

// Run starts the server and blocks until interrupted (SIGINT/SIGTERM).
func Run(ctx context.Context, cfg Config) error {
	server, err := Start(ctx, cfg)
	if err != nil {
		return err
	}

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve()
	}()

	select {
	case sig := <-sigCh:
		log.Printf("[%s] Received signal %v, shutting down", cfg.ShardID, sig)
		server.Stop()
		return nil
	case err := <-errCh:
		return err
	case <-ctx.Done():
		server.Stop()
		return ctx.Err()
	}
}

// staticStore is a read-only store backed by a pre-generated slice of tuples.
// It implements both store.Store and store.ChangeStream.
type staticStore struct {
	tuples      []store.Tuple
	currentTime store.StoreTime
}

func newStaticStore(tuples []store.Tuple) *staticStore {
	// Use length as the "current time" - each tuple is a unique point in time
	return &staticStore{
		tuples:      tuples,
		currentTime: store.StoreTime(len(tuples)),
	}
}

// WriteTuple is not supported for static store.
func (s *staticStore) WriteTuple(ctx context.Context, t store.Tuple) error {
	return fmt.Errorf("static store does not support writes")
}

// DeleteTuple is not supported for static store.
func (s *staticStore) DeleteTuple(ctx context.Context, t store.Tuple) error {
	return fmt.Errorf("static store does not support deletes")
}

// LoadAll returns an iterator over all tuples.
func (s *staticStore) LoadAll(ctx context.Context) (store.TupleIterator, error) {
	return store.NewSliceIterator(s.tuples), nil
}

// Close is a no-op.
func (s *staticStore) Close() error {
	return nil
}

// Subscribe returns channels that complete immediately (no live updates).
func (s *staticStore) Subscribe(ctx context.Context, after store.StoreTime) (<-chan store.Change, <-chan error) {
	changes := make(chan store.Change)
	errCh := make(chan error, 1)

	// Close immediately - no updates for static data
	close(changes)

	return changes, errCh
}

// CurrentTime returns the static current time.
func (s *staticStore) CurrentTime(ctx context.Context) (store.StoreTime, error) {
	return s.currentTime, nil
}

// Compile-time interface checks
var (
	_ store.Store        = (*staticStore)(nil)
	_ store.ChangeStream = (*staticStore)(nil)
)
