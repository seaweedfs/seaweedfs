package shell

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func init() {
	Commands = append(Commands, &commandS3LifecycleUnrouteTTL{})
}

type commandS3LifecycleUnrouteTTL struct{}

func (c *commandS3LifecycleUnrouteTTL) Name() string { return "s3.lifecycle.unroute-ttl" }

func (c *commandS3LifecycleUnrouteTTL) Help() string {
	return `remove filer.conf TTL volume routing for a bucket

Companion to s3.lifecycle.route-ttl. DeleteBucketLifecycle no longer
auto-strips routing — operators take it down explicitly so a transient
DELETE doesn't pull routing out from under in-flight TTL volumes.

Removes every filer.conf path-config entry whose LocationPrefix lives
under <bucketsPath>/<bucket>/ AND whose TTL is day-suffixed (the shape
route-ttl writes). Other filer.conf entries (manual operator config
unrelated to lifecycle) are left alone.

	# dry-run
	s3.lifecycle.unroute-ttl -bucket logs-prod

	# apply
	s3.lifecycle.unroute-ttl -bucket logs-prod -apply
`
}

func (c *commandS3LifecycleUnrouteTTL) HasTag(CommandTag) bool { return false }

func (c *commandS3LifecycleUnrouteTTL) Do(args []string, env *CommandEnv, writer io.Writer) error {
	fs := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	bucket := fs.String("bucket", "", "bucket name to remove TTL routing for")
	apply := fs.Bool("apply", false, "write the proposed filer.conf changes back to the filer")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *bucket == "" {
		return fmt.Errorf("-bucket required")
	}

	bucketsPath, err := resolveBucketsPath(env)
	if err != nil {
		return fmt.Errorf("resolve buckets path: %w", err)
	}

	fc, err := filer.ReadFilerConf(env.option.FilerAddress, env.option.GrpcDialOption, env.MasterClient)
	if err != nil {
		return fmt.Errorf("read filer.conf: %w", err)
	}

	removed, changed := planUnrouteTTL(fc, *bucket, bucketsPath)
	for _, line := range removed {
		fmt.Fprintln(writer, line)
	}
	if !changed {
		fmt.Fprintln(writer, "no day-TTL routing found for this bucket")
		return nil
	}
	infoAboutSimulationMode(writer, *apply, "-apply")
	if !*apply {
		var buf bytes.Buffer
		fc.ToText(&buf)
		fmt.Fprintln(writer, "proposed filer.conf:")
		fmt.Fprint(writer, buf.String())
		return nil
	}

	var buf bytes.Buffer
	if err := fc.ToText(&buf); err != nil {
		return fmt.Errorf("serialize filer.conf: %w", err)
	}
	if err := env.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer.SaveInsideFiler(context.Background(), client, filer.DirectoryEtcSeaweedFS, filer.FilerConfName, buf.Bytes())
	}); err != nil {
		return fmt.Errorf("save filer.conf: %w", err)
	}
	return nil
}

// planUnrouteTTL strips every day-TTL path-config entry rooted under the
// bucket. Returns one human-readable line per removed prefix and whether
// the FilerConf was mutated.
func planUnrouteTTL(fc *filer.FilerConf, bucket, bucketsPath string) (removed []string, changed bool) {
	bucketPrefix := fmt.Sprintf("%s/%s/", bucketsPath, bucket)
	for prefix, ttl := range fc.GetCollectionTtls(bucket) {
		if !strings.HasPrefix(prefix, bucketPrefix) || !strings.HasSuffix(ttl, "d") {
			continue
		}
		fc.DeleteLocationConf(prefix)
		removed = append(removed, fmt.Sprintf("unroute %s ttl=%s", prefix, ttl))
		changed = true
	}
	return
}
