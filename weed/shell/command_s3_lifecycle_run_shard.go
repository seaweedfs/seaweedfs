package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/dispatcher"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/scheduler"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandS3LifecycleRunShard{})
}

type commandS3LifecycleRunShard struct{}

func (c *commandS3LifecycleRunShard) Name() string {
	return "s3.lifecycle.run-shard"
}

func (c *commandS3LifecycleRunShard) Help() string {
	return `manually run one or more shards of the event-driven S3 lifecycle worker

Subscribes once to the filer meta-log, filters events to the configured
(bucket, key-prefix-hash) shards, routes them through the compiled lifecycle
engine, and dispatches due actions to the S3 server's LifecycleDelete RPC.
Persists each shard's cursor to /etc/s3/lifecycle/cursors/shard-NN.json so
subsequent runs resume.

The -shards form covers a range or set; one filer subscription handles the
whole set, with no per-shard goroutine fan-out. Provide either -shard or
-shards, not both.

	# single shard
	s3.lifecycle.run-shard -shard 0 -s3 localhost:8333 -events 100

	# contiguous range, all 16 shards via one subscription
	s3.lifecycle.run-shard -shards 0-15 -s3 localhost:8333 -events 5000

	# explicit set
	s3.lifecycle.run-shard -shards 0,3,7 -s3 localhost:8333

	# custom cadence
	s3.lifecycle.run-shard -shards 0-15 -s3 s3-host:8333 -dispatch 1s -checkpoint 10s
`
}

func (c *commandS3LifecycleRunShard) HasTag(CommandTag) bool { return false }

func (c *commandS3LifecycleRunShard) Do(args []string, env *CommandEnv, writer io.Writer) error {
	fs := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	shard := fs.Int("shard", -1, "single shard id in [0, 16); use -shards for a range or set")
	shardsSpec := fs.String("shards", "", "shard range \"lo-hi\" or comma list \"a,b,c\"; mutually exclusive with -shard")
	s3Endpoint := fs.String("s3", "", "s3 server gRPC endpoint, host:port")
	eventBudget := fs.Int("events", 1000, "max in-shard events to process before returning (0 = unbounded; counts only events that pass the shard filter)")
	dispatchTick := fs.Duration("dispatch", 5*time.Second, "dispatcher tick cadence")
	checkpointTick := fs.Duration("checkpoint", 30*time.Second, "cursor checkpoint cadence")
	refreshInterval := fs.Duration("refresh", 5*time.Minute, "interval for rebuilding the engine snapshot from filer-backed bucket configs; 0 = compile once at startup")
	runtime := fs.Duration("runtime", 0, "wall-clock cap on the run; 0 = no timeout. -events alone can hang on quiet shards")
	if err := fs.Parse(args); err != nil {
		return err
	}

	shards, err := resolveShardSelection(*shard, *shardsSpec)
	if err != nil {
		return err
	}
	if *s3Endpoint == "" {
		return fmt.Errorf("-s3 required (host:port of s3 server gRPC)")
	}
	if *eventBudget < 0 {
		return fmt.Errorf("-events must be >= 0 (0 = unbounded)")
	}

	bucketsPath, err := resolveBucketsPath(env)
	if err != nil {
		return fmt.Errorf("resolve buckets path: %w", err)
	}
	fmt.Fprintf(writer, "buckets path: %s\n", bucketsPath)

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 30*time.Second)
	conn, err := pb.GrpcDial(dialCtx, *s3Endpoint, false, env.option.GrpcDialOption)
	dialCancel()
	if err != nil {
		return fmt.Errorf("dial s3 %s: %w", *s3Endpoint, err)
	}
	defer conn.Close()
	rpcClient := s3_lifecycle_pb.NewSeaweedS3LifecycleInternalClient(conn)

	// Run the whole pipeline inside one WithFilerClient so the reader's
	// SubscribeMetadata stream and the persister share a single connection.
	return env.WithFilerClient(true, func(filerClient filer_pb.SeaweedFilerClient) error {
		eng := engine.New()

		pipeline := &dispatcher.Pipeline{
			Shards:         shards,
			BucketsPath:    bucketsPath,
			Engine:         eng,
			Persister:      &dispatcher.FilerPersister{Store: dispatcher.NewFilerStoreClient(filerClient)},
			Client:         &lifecycleClientCallable{c: rpcClient},
			FilerClient:    filerClient,
			ClientID:       util.RandomInt32(),
			ClientName:     fmt.Sprintf("shell-lifecycle-%s", formatShardLabel(shards)),
			DispatchTick:   *dispatchTick,
			CheckpointTick: *checkpointTick,
			EventBudget:    *eventBudget,
		}

		bsr := &scheduler.BucketBootstrapper{
			FilerClient: filerClient,
			BucketsPath: bucketsPath,
			Injector:    pipeline,
		}
		bootstrapCtx, bootstrapCancel := context.WithCancel(context.Background())
		defer bootstrapCancel()

		compile := func(initial bool) {
			inputs, parseErrors, err := scheduler.LoadCompileInputs(context.Background(), filerClient, bucketsPath)
			if err != nil {
				if initial {
					fmt.Fprintf(writer, "warning: load lifecycle configs: %v\n", err)
				}
				return
			}
			if initial {
				for i, pe := range parseErrors {
					if i < 3 {
						fmt.Fprintf(writer, "warning: %s: %v\n", pe.Bucket, pe.Err)
					}
				}
				if extra := len(parseErrors) - 3; extra > 0 {
					fmt.Fprintf(writer, "warning: %d additional bucket(s) had malformed lifecycle config\n", extra)
				}
				if len(inputs) == 0 {
					fmt.Fprintln(writer, "no buckets with enabled lifecycle rules at startup; will refresh and pick them up as they're added")
				} else {
					fmt.Fprintf(writer, "loaded lifecycle for %d bucket(s)\n", len(inputs))
				}
			}
			eng.Compile(inputs, engine.CompileOptions{PriorStates: scheduler.AllActivePriorStates(inputs)})
			// First time we see a bucket, spin up a one-shot walker that
			// lists existing entries and synthesizes events. The reader-
			// driven path only sees events created after the rule lands,
			// so without this backfill objects PUT before the rule (the
			// s3-tests scenario) would never expire.
			buckets := make([]string, 0, len(inputs))
			for _, in := range inputs {
				buckets = append(buckets, in.Bucket)
			}
			bsr.KickOffNew(bootstrapCtx, buckets)
		}
		compile(true)

		var ctx context.Context
		var cancel context.CancelFunc
		if *runtime > 0 {
			ctx, cancel = context.WithTimeout(context.Background(), *runtime)
		} else {
			ctx, cancel = context.WithCancel(context.Background())
		}
		defer cancel()

		// Periodic engine rebuild so rules added (or disabled) after startup
		// land in the snapshot the dispatcher reads on its next tick.
		if *refreshInterval > 0 {
			go func() {
				t := time.NewTicker(*refreshInterval)
				defer t.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-t.C:
						compile(false)
					}
				}
			}()
		}

		fmt.Fprintf(writer, "running shards %s (event budget=%d, runtime=%s, refresh=%s)…\n", formatShardLabel(shards), *eventBudget, *runtime, *refreshInterval)
		if err := pipeline.Run(ctx); err != nil {
			return fmt.Errorf("pipeline: %w", err)
		}
		fmt.Fprintf(writer, "shards %s complete; cursors checkpointed\n", formatShardLabel(shards))
		return nil
	})
}

// resolveShardSelection turns the -shard / -shards flags into a sorted,
// deduplicated []int. Exactly one form must be specified.
func resolveShardSelection(singleShard int, shardsSpec string) ([]int, error) {
	if singleShard >= 0 && shardsSpec != "" {
		return nil, fmt.Errorf("-shard and -shards are mutually exclusive")
	}
	if singleShard < 0 && shardsSpec == "" {
		return nil, fmt.Errorf("specify -shard <id> or -shards <range|set>")
	}
	if singleShard >= 0 {
		if singleShard >= s3lifecycle.ShardCount {
			return nil, fmt.Errorf("-shard %d out of [0,%d)", singleShard, s3lifecycle.ShardCount)
		}
		return []int{singleShard}, nil
	}
	return parseShardsSpec(shardsSpec)
}

// parseShardsSpec accepts "lo-hi" (inclusive) or "a,b,c" and returns a
// sorted, deduplicated, in-range []int.
func parseShardsSpec(spec string) ([]int, error) {
	spec = strings.TrimSpace(spec)
	seen := map[int]struct{}{}
	add := func(v int) error {
		if v < 0 || v >= s3lifecycle.ShardCount {
			return fmt.Errorf("shard %d out of [0,%d)", v, s3lifecycle.ShardCount)
		}
		seen[v] = struct{}{}
		return nil
	}
	if strings.Contains(spec, "-") && !strings.Contains(spec, ",") {
		parts := strings.SplitN(spec, "-", 2)
		lo, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			return nil, fmt.Errorf("range lo: %w", err)
		}
		hi, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil {
			return nil, fmt.Errorf("range hi: %w", err)
		}
		if lo > hi {
			return nil, fmt.Errorf("range lo %d > hi %d", lo, hi)
		}
		for v := lo; v <= hi; v++ {
			if err := add(v); err != nil {
				return nil, err
			}
		}
	} else {
		for _, part := range strings.Split(spec, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			v, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("shard list: %w", err)
			}
			if err := add(v); err != nil {
				return nil, err
			}
		}
	}
	if len(seen) == 0 {
		return nil, fmt.Errorf("empty shard set")
	}
	out := make([]int, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Ints(out)
	return out, nil
}

func formatShardLabel(shards []int) string {
	if len(shards) == 1 {
		return fmt.Sprintf("%d", shards[0])
	}
	// Detect contiguous range.
	contiguous := true
	for i := 1; i < len(shards); i++ {
		if shards[i] != shards[i-1]+1 {
			contiguous = false
			break
		}
	}
	if contiguous {
		return fmt.Sprintf("%d-%d", shards[0], shards[len(shards)-1])
	}
	parts := make([]string, len(shards))
	for i, v := range shards {
		parts[i] = strconv.Itoa(v)
	}
	return strings.Join(parts, ",")
}

// lifecycleClientCallable adapts the generated grpc client (variadic
// CallOption tail) to the dispatcher.LifecycleClient interface.
type lifecycleClientCallable struct {
	c s3_lifecycle_pb.SeaweedS3LifecycleInternalClient
}

func (l *lifecycleClientCallable) LifecycleDelete(ctx context.Context, req *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	return l.c.LifecycleDelete(ctx, req)
}

// resolveBucketsPath fetches the filer's configured buckets directory.
// Falls back to /buckets when the filer doesn't return one.
func resolveBucketsPath(env *CommandEnv) (string, error) {
	var path string
	err := env.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return err
		}
		path = resp.GetDirBuckets()
		return nil
	})
	if err != nil {
		return "", err
	}
	if path == "" {
		path = "/buckets"
	}
	return path, nil
}

