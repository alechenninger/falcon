// Command falcon runs a falcon authorization server node.
//
// Usage:
//
//	falcon --shard-id=shard-0 --listen=:50051 --peers=shard-1=localhost:50052
//
// Flags:
//
//	--shard-id      Unique identifier for this shard (required)
//	--listen        Address to listen on (default ":50051")
//	--peers         Comma-separated list of peer shardID=addr pairs
//	--num-shards    Total number of shards in the cluster (default 2)
//	--small         Use small test data config (for quick testing)
//	--verbose, -v   Enable verbose (debug) logging
//	--pprof         Address for pprof HTTP server (e.g., ":6060")
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof" // Register pprof handlers
	"os"

	"github.com/alechenninger/falcon/graph"
	"github.com/alechenninger/falcon/server"
)

func main() {
	var (
		shardID   = flag.String("shard-id", "", "Unique identifier for this shard (required)")
		listen    = flag.String("listen", ":50051", "Address to listen on")
		peers     = flag.String("peers", "", "Comma-separated list of peer shardID=addr pairs")
		numShards = flag.Int("num-shards", 2, "Total number of shards in the cluster")
		small     = flag.Bool("small", false, "Use small test data config (~238 tuples)")
		medium    = flag.Bool("medium", false, "Use medium test data config (~20K tuples)")
		verbose   = flag.Bool("verbose", false, "Enable verbose (debug) logging")
		pprof     = flag.String("pprof", "", "Address for pprof HTTP server (e.g., :6060)")
	)
	flag.Bool("v", false, "Enable verbose (debug) logging (shorthand)")
	flag.Parse()

	// Start pprof server if requested
	if *pprof != "" {
		go func() {
			log.Printf("Starting pprof server on %s", *pprof)
			if err := http.ListenAndServe(*pprof, nil); err != nil {
				log.Printf("pprof server error: %v", err)
			}
		}()
	}

	// Handle -v shorthand
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "v" {
			*verbose = true
		}
	})

	if *shardID == "" {
		fmt.Fprintln(os.Stderr, "error: --shard-id is required")
		flag.Usage()
		os.Exit(1)
	}

	peerMap, err := server.ParsePeers(*peers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing peers: %v\n", err)
		os.Exit(1)
	}

	testDataCfg := graph.DefaultTestDataConfig()
	if *small {
		testDataCfg = graph.SmallTestDataConfig()
	} else if *medium {
		testDataCfg = graph.MediumTestDataConfig()
	}

	cfg := server.Config{
		ShardID:        graph.ShardID(*shardID),
		ListenAddr:     *listen,
		Peers:          peerMap,
		NumShards:      *numShards,
		TestDataConfig: testDataCfg,
		Verbose:        *verbose,
	}

	log.SetFlags(log.Ltime | log.Lmicroseconds)

	ctx := context.Background()
	if err := server.Run(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
