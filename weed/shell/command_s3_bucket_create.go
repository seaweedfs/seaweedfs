package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3bucket"
)

func init() {
	Commands = append(Commands, &commandS3BucketCreate{})
}

type commandS3BucketCreate struct {
}

func (c *commandS3BucketCreate) Name() string {
	return "s3.bucket.create"
}

func (c *commandS3BucketCreate) Help() string {
	return `create a bucket with a given name

	Example:
		s3.bucket.create -name <bucket_name>
		s3.bucket.create -name <bucket_name> -owner <identity_name>
		s3.bucket.create -name <bucket_name> -withLock

	The -owner flag sets the bucket owner identity. This is important when using
	S3 IAM authentication, as non-admin users can only access buckets they own.
	If not specified, the bucket will have no owner and will only be accessible
	by admin users.

	The -owner value should match the identity name configured in your S3 IAM
	system (the "name" field in s3.json identities configuration).

	The -withLock flag enables S3 Object Lock on the bucket. This provides WORM
	(Write Once Read Many) protection for objects. Once enabled, Object Lock
	cannot be disabled. Versioning is automatically enabled when using this flag.
`
}

func (c *commandS3BucketCreate) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3BucketCreate) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	bucketCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	bucketName := bucketCommand.String("name", "", "bucket name")
	bucketOwner := bucketCommand.String("owner", "", "bucket owner identity name (for S3 IAM authentication)")
	withLock := bucketCommand.Bool("withLock", false, "enable Object Lock on the bucket (requires and enables versioning)")
	if err = bucketCommand.Parse(args); err != nil {
		return nil
	}

	if *bucketName == "" {
		return fmt.Errorf("empty bucket name")
	}

	err = s3bucket.VerifyS3BucketName(*bucketName)
	if err != nil {
		return err
	}

	// Trim whitespace from owner and treat whitespace-only as empty
	owner := strings.TrimSpace(*bucketOwner)

	err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return fmt.Errorf("get filer configuration: %w", err)
		}
		filerBucketsPath := resp.DirBuckets

		fmt.Fprintln(writer, "create bucket under", filerBucketsPath)

		entry := &filer_pb.Entry{
			Name:        *bucketName,
			IsDirectory: true,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    time.Now().Unix(),
				Crtime:   time.Now().Unix(),
				FileMode: uint32(0777 | os.ModeDir),
			},
		}

		// Set bucket owner if specified
		if owner != "" {
			if entry.Extended == nil {
				entry.Extended = make(map[string][]byte)
			}
			entry.Extended[s3_constants.AmzIdentityId] = []byte(owner)
		}

		// Enable Object Lock if specified
		if *withLock {
			if entry.Extended == nil {
				entry.Extended = make(map[string][]byte)
			}
			// Enable versioning (required for Object Lock)
			entry.Extended[s3_constants.ExtVersioningKey] = []byte(s3_constants.VersioningEnabled)
			// Enable Object Lock
			entry.Extended[s3_constants.ExtObjectLockEnabledKey] = []byte(s3_constants.ObjectLockEnabled)
		}

		if _, err := client.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
			Directory: filerBucketsPath,
			Entry:     entry,
		}); err != nil {
			return err
		}

		fmt.Fprintln(writer, "created bucket", *bucketName)
		if owner != "" {
			fmt.Fprintln(writer, "bucket owner:", owner)
		}
		if *withLock {
			fmt.Fprintln(writer, "Object Lock: enabled")
			fmt.Fprintln(writer, "Versioning: enabled")
		}

		return nil

	})

	return err

}
