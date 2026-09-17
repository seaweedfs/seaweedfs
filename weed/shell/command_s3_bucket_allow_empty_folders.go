package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3bucket"
)

func init() {
	Commands = append(Commands, &commandS3BucketAllowEmptyFolders{})
}

type commandS3BucketAllowEmptyFolders struct {
}

func (c *commandS3BucketAllowEmptyFolders) Name() string {
	return "s3.bucket.allowEmptyFolders"
}

func (c *commandS3BucketAllowEmptyFolders) Help() string {
	return `view or toggle whether a bucket keeps empty folders

	When enabled, the empty folder cleaner skips this bucket, so empty
	directories persist (POSIX semantics). weed mount sets this when
	mounting a bucket root.

	When disabled, folders emptied by object deletes are removed
	asynchronously and stop appearing as CommonPrefix in S3 listings.
	The explicit "false" marker is kept by weed mount.

	Example:
		# Show the current setting
		s3.bucket.allowEmptyFolders -name <bucket_name>

		# Keep empty folders (skip the cleaner for this bucket)
		s3.bucket.allowEmptyFolders -name <bucket_name> -enable

		# Let the cleaner remove empty folders
		s3.bucket.allowEmptyFolders -name <bucket_name> -disable
`
}

func (c *commandS3BucketAllowEmptyFolders) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3BucketAllowEmptyFolders) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	bucketCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	bucketName := bucketCommand.String("name", "", "bucket name")
	enable := bucketCommand.Bool("enable", false, "keep empty folders in the bucket")
	disable := bucketCommand.Bool("disable", false, "remove empty folders asynchronously")
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

	filerBucketsPath, err := readFilerBucketsPath(commandEnv)
	if err != nil {
		return fmt.Errorf("read buckets: %w", err)
	}

	return commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		for attempt := 0; attempt < 5; attempt++ {
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
				if strings.EqualFold(strings.TrimSpace(string(entry.Extended[s3_constants.ExtAllowEmptyFolders])), "true") {
					state = "enabled"
				}
				fmt.Fprintf(writer, "Bucket: %s\n", *bucketName)
				fmt.Fprintf(writer, "Allow empty folders: %s\n", state)
				return nil
			}

			expected := filer_pb.SnapshotExtended(entry.Extended, s3_constants.ExtAllowEmptyFolders)

			if entry.Extended == nil {
				entry.Extended = make(map[string][]byte)
			}
			state := "disabled"
			if *enable {
				entry.Extended[s3_constants.ExtAllowEmptyFolders] = []byte("true")
				state = "enabled"
			} else {
				entry.Extended[s3_constants.ExtAllowEmptyFolders] = []byte("false")
			}

			if _, err := client.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{
				Directory:        filerBucketsPath,
				Entry:            entry,
				ExpectedExtended: expected,
			}); err != nil {
				if status.Code(err) == codes.FailedPrecondition {
					continue
				}
				return fmt.Errorf("failed to update bucket: %w", err)
			}

			fmt.Fprintf(writer, "Bucket %s allow empty folders %s\n", *bucketName, state)
			return nil
		}
		return fmt.Errorf("bucket %s changed concurrently; please retry", *bucketName)
	})
}
