package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/lifecycle_xml"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/dispatcher"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
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
	return `manually run one shard of the event-driven S3 lifecycle worker

Subscribes to the filer meta-log filtered to the given (bucket, key-prefix-hash)
shard, routes events through the compiled lifecycle engine, and dispatches due
actions to the S3 server's LifecycleDelete RPC. Persists the per-shard cursor
to /etc/s3/lifecycle/cursors/shard-NN.json so subsequent runs resume.

	# run shard 0 against an S3 server, bound at 100 events
	s3.lifecycle.run-shard -shard 0 -s3 localhost:8333 -events 100

	# run shard 7 with custom dispatch tick + checkpoint cadence
	s3.lifecycle.run-shard -shard 7 -s3 s3-host:8333 -dispatch 1s -checkpoint 10s
`
}

func (c *commandS3LifecycleRunShard) HasTag(CommandTag) bool { return false }

func (c *commandS3LifecycleRunShard) Do(args []string, env *CommandEnv, writer io.Writer) error {
	fs := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	shard := fs.Int("shard", -1, "shard id in [0, 16)")
	s3Endpoint := fs.String("s3", "", "s3 server gRPC endpoint, host:port")
	eventBudget := fs.Int("events", 1000, "max events to process before returning (0 = unbounded)")
	dispatchTick := fs.Duration("dispatch", 5*time.Second, "dispatcher tick cadence")
	checkpointTick := fs.Duration("checkpoint", 30*time.Second, "cursor checkpoint cadence")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *shard < 0 || *shard >= s3lifecycle.ShardCount {
		return fmt.Errorf("-shard required in [0,%d)", s3lifecycle.ShardCount)
	}
	if *s3Endpoint == "" {
		return fmt.Errorf("-s3 required (host:port of s3 server gRPC)")
	}

	bucketsPath, err := resolveBucketsPath(env)
	if err != nil {
		return fmt.Errorf("resolve buckets path: %w", err)
	}
	fmt.Fprintf(writer, "buckets path: %s\n", bucketsPath)

	conn, err := pb.GrpcDial(context.Background(), *s3Endpoint, false, env.option.GrpcDialOption)
	if err != nil {
		return fmt.Errorf("dial s3 %s: %w", *s3Endpoint, err)
	}
	defer conn.Close()
	rpcClient := s3_lifecycle_pb.NewSeaweedS3LifecycleInternalClient(conn)

	// Run the whole pipeline inside one WithFilerClient so the reader's
	// SubscribeMetadata stream and the persister share a single connection.
	return env.WithFilerClient(true, func(filerClient filer_pb.SeaweedFilerClient) error {
		inputs, err := loadLifecycleCompileInputs(context.Background(), filerClient, bucketsPath)
		if err != nil {
			return fmt.Errorf("load lifecycle configs: %w", err)
		}
		if len(inputs) == 0 {
			fmt.Fprintln(writer, "no buckets with enabled lifecycle rules found")
			return nil
		}
		fmt.Fprintf(writer, "loaded lifecycle for %d bucket(s)\n", len(inputs))

		// Activate every action so this manual run dispatches whatever fires.
		// The production bootstrap walker promotes actions only after a clean
		// walk; this shell entrypoint runs out-of-band of that flow.
		eng := engine.New()
		eng.Compile(inputs, engine.CompileOptions{PriorStates: allActivePriorStates(inputs)})

		pipeline := &dispatcher.Pipeline{
			ShardID:        *shard,
			BucketsPath:    bucketsPath,
			Engine:         eng,
			Cursor:         reader.NewCursor(),
			Persister:      &dispatcher.FilerPersister{Store: dispatcher.NewFilerStoreClient(filerClient)},
			Client:         &lifecycleClientCallable{c: rpcClient},
			FilerClient:    filerClient,
			ClientID:       util.RandomInt32(),
			ClientName:     fmt.Sprintf("shell-lifecycle-shard-%02d", *shard),
			DispatchTick:   *dispatchTick,
			CheckpointTick: *checkpointTick,
			EventBudget:    *eventBudget,
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		fmt.Fprintf(writer, "running shard %d (event budget=%d)…\n", *shard, *eventBudget)
		if err := pipeline.Run(ctx); err != nil {
			return fmt.Errorf("pipeline: %w", err)
		}
		fmt.Fprintf(writer, "shard %d complete; cursor checkpointed\n", *shard)
		return nil
	})
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

// loadLifecycleCompileInputs walks the buckets directory and reads each
// bucket entry's lifecycle XML from its Extended attributes.
func loadLifecycleCompileInputs(ctx context.Context, client filer_pb.SeaweedFilerClient, bucketsPath string) ([]engine.CompileInput, error) {
	var inputs []engine.CompileInput
	err := filer_pb.SeaweedList(ctx, client, bucketsPath, "", func(entry *filer_pb.Entry, isLast bool) error {
		if !entry.IsDirectory {
			return nil
		}
		xmlBytes, ok := entry.Extended[bucketLifecycleConfigurationXMLKey]
		if !ok || len(xmlBytes) == 0 {
			return nil
		}
		rules, err := lifecycle_xml.ParseCanonical(xmlBytes)
		if err != nil || len(rules) == 0 {
			// Skip buckets with malformed configs; report and continue.
			return nil
		}
		inputs = append(inputs, engine.CompileInput{
			Bucket:    entry.Name,
			Rules:     rules,
			Versioned: isBucketVersioned(entry),
		})
		return nil
	}, "", false, 4096)
	if err != nil {
		return nil, err
	}
	return inputs, nil
}

const bucketLifecycleConfigurationXMLKey = "s3-bucket-lifecycle-configuration-xml"

func isBucketVersioned(entry *filer_pb.Entry) bool {
	v, ok := entry.Extended[s3_constants.ExtVersioningKey]
	if !ok {
		return false
	}
	s := strings.ToLower(strings.TrimSpace(string(v)))
	return s == "enabled" || s == "suspended"
}

// allActivePriorStates seeds every compiled action as bootstrap-complete +
// event-driven so the run dispatches whatever fires. Production bootstrap
// walks set this incrementally per bucket; this manual run skips the walk.
func allActivePriorStates(inputs []engine.CompileInput) map[s3lifecycle.ActionKey]engine.PriorState {
	prior := map[s3lifecycle.ActionKey]engine.PriorState{}
	for _, in := range inputs {
		for _, rule := range in.Rules {
			hash := s3lifecycle.RuleHash(rule)
			for _, kind := range s3lifecycle.RuleActionKinds(rule) {
				key := s3lifecycle.ActionKey{Bucket: in.Bucket, RuleHash: hash, ActionKind: kind}
				prior[key] = engine.PriorState{
					BootstrapComplete: true,
					Mode:              engine.ModeEventDriven,
				}
			}
		}
	}
	return prior
}
