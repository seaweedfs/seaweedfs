package shell

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3bucket"
)

func init() {
	Commands = append(Commands, &commandS3BucketLifecycleFastpath{})
}

type commandS3BucketLifecycleFastpath struct {
}

func (c *commandS3BucketLifecycleFastpath) Name() string {
	return "s3.bucket.lifecycle.fastpath"
}

func (c *commandS3BucketLifecycleFastpath) Help() string {
	return `view or toggle the per-bucket lifecycle TTL fast path

	When enabled, an Expiration.Days lifecycle rule is stamped as a volume
	TTL at PutObject time, so the volume server reclaims the data on its own
	and the lifecycle worker skips per-chunk deletes. Off by default: a
	volume TTL is baked into the object at write time and can't honor a later
	policy change (rule removed or lengthened), unlike worker-driven
	expiration. The fast path never applies to versioned or object-locked
	buckets regardless of this flag.

	Example:
		# Show the current setting
		s3.bucket.lifecycle.fastpath -name <bucket_name>

		# Enable
		s3.bucket.lifecycle.fastpath -name <bucket_name> -enable

		# Disable
		s3.bucket.lifecycle.fastpath -name <bucket_name> -disable
`
}

func (c *commandS3BucketLifecycleFastpath) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3BucketLifecycleFastpath) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	bucketCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	bucketName := bucketCommand.String("name", "", "bucket name")
	enable := bucketCommand.Bool("enable", false, "enable the lifecycle TTL fast path")
	disable := bucketCommand.Bool("disable", false, "disable the lifecycle TTL fast path")
	if err = bucketCommand.Parse(args); err != nil {
		return err
	}

	if *bucketName == "" {
		return fmt.Errorf("empty bucket name")
	}
	if err := s3bucket.VerifyS3BucketName(*bucketName); err != nil {
		return fmt.Errorf("invalid bucket name %q: %w", *bucketName, err)
	}
	if *enable && *disable {
		return fmt.Errorf("only one of -enable or -disable can be set")
	}

	return commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return fmt.Errorf("get filer configuration: %w", err)
		}
		filerBucketsPath := resp.DirBuckets

		lookupResp, err := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: filerBucketsPath,
			Name:      *bucketName,
		})
		if err != nil {
			return fmt.Errorf("lookup bucket %s: %w", *bucketName, err)
		}
		entry := lookupResp.Entry

		if !*enable && !*disable {
			state := "disabled"
			if string(entry.Extended[s3_constants.ExtLifecycleTtlFastPathKey]) == "true" {
				state = "enabled"
			}
			fmt.Fprintf(writer, "Bucket: %s\n", *bucketName)
			fmt.Fprintf(writer, "Lifecycle TTL fast path: %s\n", state)
			return nil
		}

		if entry.Extended == nil {
			entry.Extended = make(map[string][]byte)
		}
		state := "disabled"
		if *enable {
			entry.Extended[s3_constants.ExtLifecycleTtlFastPathKey] = []byte("true")
			state = "enabled"
		} else {
			delete(entry.Extended, s3_constants.ExtLifecycleTtlFastPathKey)
		}

		if _, err := client.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{
			Directory: filerBucketsPath,
			Entry:     entry,
		}); err != nil {
			return fmt.Errorf("failed to update bucket: %w", err)
		}

		fmt.Fprintf(writer, "Bucket %s lifecycle TTL fast path %s\n", *bucketName, state)
		return nil
	})
}
