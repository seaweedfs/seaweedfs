package shell

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandS3BucketOwner{})
}

type commandS3BucketOwner struct {
}

func (c *commandS3BucketOwner) Name() string {
	return "s3.bucket.owner"
}

func (c *commandS3BucketOwner) Help() string {
	return `view or change the owner of an S3 bucket

	Example:
		# View the current owner of a bucket
		s3.bucket.owner -name <bucket_name>

		# Set or change the owner of a bucket
		s3.bucket.owner -name <bucket_name> -owner <identity_name>

		# Remove the owner (make bucket admin-only)
		s3.bucket.owner -name <bucket_name> -owner ""

	The owner identity determines which S3 user can access the bucket.
	Non-admin users can only access buckets they own. Admin users can
	access all buckets regardless of ownership.
`
}

func (c *commandS3BucketOwner) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3BucketOwner) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	bucketCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	bucketName := bucketCommand.String("name", "", "bucket name")
	bucketOwner := bucketCommand.String("owner", "", "new bucket owner identity name")
	setOwner := bucketCommand.Bool("set", false, "set the owner (required when changing owner)")
	if err = bucketCommand.Parse(args); err != nil {
		return nil
	}

	if *bucketName == "" {
		return fmt.Errorf("empty bucket name")
	}

	err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return fmt.Errorf("get filer configuration: %w", err)
		}
		filerBucketsPath := resp.DirBuckets

		// Look up the bucket entry
		lookupResp, err := filer_pb.LookupEntry(context.Background(), client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: filerBucketsPath,
			Name:      *bucketName,
		})
		if err != nil {
			return fmt.Errorf("lookup bucket %s: %w", *bucketName, err)
		}

		entry := lookupResp.Entry

		// If -set flag is provided, update the owner
		if *setOwner {
			if entry.Extended == nil {
				entry.Extended = make(map[string][]byte)
			}

			if *bucketOwner == "" {
				// Remove owner
				delete(entry.Extended, s3_constants.AmzIdentityId)
				fmt.Fprintf(writer, "Removing owner from bucket %s\n", *bucketName)
			} else {
				// Set new owner
				entry.Extended[s3_constants.AmzIdentityId] = []byte(*bucketOwner)
				fmt.Fprintf(writer, "Setting owner of bucket %s to: %s\n", *bucketName, *bucketOwner)
			}

			// Update the entry
			if err := filer_pb.UpdateEntry(context.Background(), client, &filer_pb.UpdateEntryRequest{
				Directory: filerBucketsPath,
				Entry:     entry,
			}); err != nil {
				return fmt.Errorf("failed to update bucket: %w", err)
			}

			fmt.Fprintf(writer, "Bucket owner updated successfully.\n")
			return nil
		}

		// Display current owner
		fmt.Fprintf(writer, "Bucket: %s\n", *bucketName)
		fmt.Fprintf(writer, "Path: %s\n", util.NewFullPath(filerBucketsPath, *bucketName))

		if entry.Extended != nil {
			if owner, ok := entry.Extended[s3_constants.AmzIdentityId]; ok && len(owner) > 0 {
				fmt.Fprintf(writer, "Owner: %s\n", string(owner))
			} else {
				fmt.Fprintf(writer, "Owner: (none - admin access only)\n")
			}
		} else {
			fmt.Fprintf(writer, "Owner: (none - admin access only)\n")
		}

		return nil
	})

	return err
}

