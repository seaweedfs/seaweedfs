package shell

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"sort"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/lifecycle_xml"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/scheduler"
)

func init() {
	Commands = append(Commands, &commandS3LifecycleRouteTTL{})
}

type commandS3LifecycleRouteTTL struct{}

func (c *commandS3LifecycleRouteTTL) Name() string { return "s3.lifecycle.route-ttl" }

func (c *commandS3LifecycleRouteTTL) Help() string {
	return `set up filer.conf TTL volume routing for a bucket's lifecycle policy

PutBucketLifecycleConfiguration no longer touches filer.conf — it just
validates and stores the XML. Volume-TTL routing is now an explicit
operator step: this command reads the bucket's lifecycle XML and writes
one filer.conf path-config entry per simple Expiration.Days rule, so new
writes under that prefix land on TTL volumes and the volume server
handles physical expiration. Companion to s3.lifecycle.unroute-ttl.

	# dry-run: show what would change
	s3.lifecycle.route-ttl -bucket logs-prod

	# apply
	s3.lifecycle.route-ttl -bucket logs-prod -apply

Versioned buckets are skipped: TTL volumes expire as a unit, which would
destroy noncurrent versions; the lifecycle worker drives expiration for
versioned buckets.
`
}

func (c *commandS3LifecycleRouteTTL) HasTag(CommandTag) bool { return false }

func (c *commandS3LifecycleRouteTTL) Do(args []string, env *CommandEnv, writer io.Writer) error {
	fs := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	bucket := fs.String("bucket", "", "bucket name to set up TTL routing for")
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

	var bucketEntry *filer_pb.Entry
	if err := env.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := filer_pb.LookupEntry(context.Background(), client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: bucketsPath,
			Name:      *bucket,
		})
		if err != nil {
			return err
		}
		bucketEntry = resp.Entry
		return nil
	}); err != nil {
		return fmt.Errorf("lookup bucket %s: %w", *bucket, err)
	}
	if bucketEntry == nil {
		return fmt.Errorf("bucket %s not found", *bucket)
	}

	xmlBytes, ok := bucketEntry.Extended[scheduler.BucketLifecycleConfigurationXMLKey]
	if !ok || len(xmlBytes) == 0 {
		return fmt.Errorf("bucket %s has no lifecycle configuration", *bucket)
	}

	if scheduler.IsBucketVersioned(bucketEntry) {
		fmt.Fprintf(writer, "bucket %s is versioned; TTL volumes would destroy noncurrent versions, so volume routing is not applied. The lifecycle worker handles expiration.\n", *bucket)
		return nil
	}

	rules, err := lifecycle_xml.ParseCanonical(xmlBytes)
	if err != nil {
		return fmt.Errorf("parse lifecycle xml for %s: %w", *bucket, err)
	}

	fc, err := filer.ReadFilerConf(env.option.FilerAddress, env.option.GrpcDialOption, env.MasterClient)
	if err != nil {
		return fmt.Errorf("read filer.conf: %w", err)
	}

	added, skipped, changed := planRouteTTL(fc, *bucket, bucketsPath, rules)
	for _, line := range added {
		fmt.Fprintln(writer, line)
	}
	for _, line := range skipped {
		fmt.Fprintln(writer, line)
	}
	if !changed {
		fmt.Fprintln(writer, "no filer.conf changes needed")
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

// planRouteTTL inspects the bucket's compiled rules against the live
// filer.conf and adds a path-config entry per simple Expiration.Days rule.
// Returns human-readable lines for the added / skipped rules and whether the
// FilerConf was mutated.
func planRouteTTL(fc *filer.FilerConf, bucket, bucketsPath string, rules []*s3lifecycle.Rule) (added, skipped []string, changed bool) {
	collection := bucket
	// Mirror PutBucketLifecycleConfigurationHandler: stable iteration so
	// the dry-run output diffs cleanly against re-runs.
	sortRulesByID(rules)
	for _, rule := range rules {
		if rule.Status != s3lifecycle.StatusEnabled {
			skipped = append(skipped, fmt.Sprintf("skip rule %q: status=%s", rule.ID, rule.Status))
			continue
		}
		if rule.ExpirationDays <= 0 {
			skipped = append(skipped, fmt.Sprintf("skip rule %q: no Expiration.Days (worker handles non-TTL kinds)", rule.ID))
			continue
		}
		if len(rule.FilterTags) > 0 || rule.FilterSizeGreaterThan > 0 || rule.FilterSizeLessThan > 0 {
			skipped = append(skipped, fmt.Sprintf("skip rule %q: tag / size filter (worker evaluates these per object)", rule.ID))
			continue
		}
		locationPrefix := fmt.Sprintf("%s/%s/%s", bucketsPath, bucket, rule.Prefix)
		ttl := fmt.Sprintf("%dd", rule.ExpirationDays)
		if existing, ok := fc.GetLocationConf(locationPrefix); ok && existing.Ttl == ttl {
			skipped = append(skipped, fmt.Sprintf("rule %q: %s already routed at TTL=%s", rule.ID, locationPrefix, ttl))
			continue
		}
		conf := &filer_pb.FilerConf_PathConf{
			LocationPrefix: locationPrefix,
			Collection:     collection,
			Ttl:            ttl,
			// DataCenter / Rack / DataNode intentionally omitted: S3 is
			// not pinned to one location; inheriting filer-level defaults
			// is safer than nailing routes here.
		}
		if err := fc.AddLocationConf(conf); err != nil {
			skipped = append(skipped, fmt.Sprintf("rule %q: AddLocationConf %s: %v", rule.ID, locationPrefix, err))
			continue
		}
		added = append(added, fmt.Sprintf("route rule %q: %s ttl=%s", rule.ID, locationPrefix, ttl))
		changed = true
	}
	return
}

func sortRulesByID(rules []*s3lifecycle.Rule) {
	sort.Slice(rules, func(i, j int) bool { return rules[i].ID < rules[j].ID })
}
