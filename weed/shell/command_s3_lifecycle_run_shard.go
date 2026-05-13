package shell

import (
	"context"
	"errors"
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
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/dailyrun"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/dispatcher"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/scheduler"
)

func init() {
	Commands = append(Commands, &commandS3LifecycleRunShard{})
}

type commandS3LifecycleRunShard struct{}

func (c *commandS3LifecycleRunShard) Name() string {
	return "s3.lifecycle.run-shard"
}

func (c *commandS3LifecycleRunShard) Help() string {
	return `manually run one daily-replay pass for the given shards

Drives dailyrun.Run once against the live filer + S3 server: builds
the engine snapshot from filer-backed bucket configs, opens the
meta-log subscription per shard, dispatches due actions via
LifecycleDelete, and walks the live tree for any walker-bound rules.
Persists each shard's cursor to /etc/s3/lifecycle/daily-cursors/
so subsequent runs resume.

Used by the s3-tests CI workflow and the test/s3/lifecycle/
integration tests to drive expirations on demand without standing up
the full admin+worker plugin stack.

	# single shard
	s3.lifecycle.run-shard -shard 0 -s3 localhost:8333 -events 100

	# contiguous range
	s3.lifecycle.run-shard -shards 0-15 -s3 localhost:8333 -events 5000

	# explicit set
	s3.lifecycle.run-shard -shards 0,3,7 -s3 localhost:8333

	# bounded wall-clock
	s3.lifecycle.run-shard -shards 0-15 -s3 localhost:8333 -runtime 10s
`
}

func (c *commandS3LifecycleRunShard) HasTag(CommandTag) bool { return false }

func (c *commandS3LifecycleRunShard) Do(args []string, env *CommandEnv, writer io.Writer) error {
	fs := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	shard := fs.Int("shard", -1, "single shard id in [0, 16); use -shards for a range or set")
	shardsSpec := fs.String("shards", "", "shard range \"lo-hi\" or comma list \"a,b,c\"; mutually exclusive with -shard")
	s3Endpoint := fs.String("s3", "", "s3 server gRPC endpoint, host:port")
	eventBudget := fs.Int("events", 1000, "max in-shard events per pass (0 = drain to now)")
	runtime := fs.Duration("runtime", 0, "wall-clock cap on the whole run; 0 = no timeout")
	// -refresh drives the inter-pass cadence when the command runs as a
	// long-lived worker (the s3tests CI workflow case): every refresh
	// the engine snapshot is re-loaded and dailyrun.Run fires another
	// pass. 0 means "run once and exit" (the integration-test case).
	cadence := fs.Duration("refresh", 0, "inter-pass interval; 0 = single pass, then exit")
	// Obsolete flags kept for back-compat with existing CI scripts and
	// integration tests. Accept and ignore.
	_ = fs.Duration("dispatch", 0, "ignored (legacy streaming flag)")
	_ = fs.Duration("checkpoint", 0, "ignored (legacy streaming flag)")
	_ = fs.Duration("bootstrap-interval", 0, "ignored (legacy streaming flag)")
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

	return env.WithFilerClient(true, func(filerClient filer_pb.SeaweedFilerClient) error {
		ctx := context.Background()
		var cancel context.CancelFunc
		if *runtime > 0 {
			ctx, cancel = context.WithTimeout(ctx, *runtime)
			defer cancel()
		}
		client := &lifecycleClientCallable{c: rpcClient}
		listFn := dailyrun.FilerListFunc(filerClient, bucketsPath)
		walkerDispatch := &dailyrun.WalkerDispatcher{Client: client}

		fmt.Fprintf(writer, "running shards %s (event budget=%d, runtime=%s, refresh=%s)…\n",
			formatShardLabel(shards), *eventBudget, *runtime, *cadence)

		announcedLoad := false
		runPass := func() error {
			eng := engine.New()
			inputs, parseErrors, err := scheduler.LoadCompileInputs(ctx, filerClient, bucketsPath)
			if err != nil {
				return fmt.Errorf("load lifecycle configs: %w", err)
			}
			for i, pe := range parseErrors {
				if i < 3 {
					fmt.Fprintf(writer, "warning: %s: %v\n", pe.Bucket, pe.Err)
				}
			}
			if extra := len(parseErrors) - 3; extra > 0 {
				fmt.Fprintf(writer, "warning: %d additional bucket(s) had malformed lifecycle config\n", extra)
			}
			eng.Compile(inputs, engine.CompileOptions{PriorStates: scheduler.AllActivePriorStates(inputs)})
			if len(inputs) == 0 {
				return nil
			}
			if !announcedLoad {
				fmt.Fprintf(writer, "loaded lifecycle for %d bucket(s)\n", len(inputs))
				announcedLoad = true
			}
			buckets := make([]string, 0, len(inputs))
			for _, in := range inputs {
				if in.Bucket != "" {
					buckets = append(buckets, in.Bucket)
				}
			}
			walker := dailyrun.WalkerFunc(func(walkCtx context.Context, view *engine.Snapshot, shardID int) error {
				return dailyrun.WalkBuckets(walkCtx, view, shardID, buckets, listFn, walkerDispatch)
			})
			return dailyrun.Run(ctx, dailyrun.Config{
				Shards:      shards,
				BucketsPath: bucketsPath,
				Engine:      eng,
				FilerClient: filerClient,
				Client:      client,
				Persister:   &dailyrun.FilerCursorPersister{Store: dispatcher.NewFilerStoreClient(filerClient)},
				Lister:      dispatcher.NewFilerSiblingLister(filerClient, bucketsPath),
				// The shell command is used for bounded one-shot sweeps in
				// integration tests and CI. Fan out across the selected shards
				// so recovery walks do not serialize 16 shard scans into a 10s
				// timeout budget.
				Workers: len(shards),
				Walker:      walker,
				EventBudget: *eventBudget,
				ClientName:  fmt.Sprintf("shell-lifecycle-%s", formatShardLabel(shards)),
			})
		}

		if err := runPass(); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("daily_run: %w", err)
		}
		// cadence=0 → one-shot (test/s3/lifecycle/ uses this).
		// cadence>0 → loop until ctx done (s3tests CI workflow uses this).
		if *cadence > 0 {
			ticker := time.NewTicker(*cadence)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					fmt.Fprintf(writer, "shards %s complete; ctx done\n", formatShardLabel(shards))
					return nil
				case <-ticker.C:
					if err := runPass(); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						fmt.Fprintf(writer, "shards %s pass error: %v\n", formatShardLabel(shards), err)
					}
				}
			}
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
// CallOption tail) to dailyrun.LifecycleClient.
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
