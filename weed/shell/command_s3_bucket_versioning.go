package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func init() {
	Commands = append(Commands, &commandS3BucketVersioning{})
}

type commandS3BucketVersioning struct {
}

func (c *commandS3BucketVersioning) Name() string {
	return "s3.bucket.versioning"
}

func (c *commandS3BucketVersioning) Help() string {
	return `view or update S3 bucket versioning configuration

	Example:
		# View the current versioning status
		s3.bucket.versioning -name <bucket_name>

		# Enable versioning
		s3.bucket.versioning -name <bucket_name> -enable

		# Suspend versioning
		s3.bucket.versioning -name <bucket_name> -suspend

		# Set versioning status explicitly (Enabled or Suspended)
		s3.bucket.versioning -name <bucket_name> -status Enabled

	Object Lock requires versioning to be enabled and prevents suspending it.
`
}

func (c *commandS3BucketVersioning) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3BucketVersioning) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	bucketCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	bucketName := bucketCommand.String("name", "", "bucket name")
	statusFlag := bucketCommand.String("status", "", "versioning status: Enabled or Suspended")
	enable := bucketCommand.Bool("enable", false, "enable versioning on the bucket")
	suspend := bucketCommand.Bool("suspend", false, "suspend versioning on the bucket")
	if err = bucketCommand.Parse(args); err != nil {
		return err
	}

	if *bucketName == "" {
		return fmt.Errorf("empty bucket name")
	}

	desiredStatus, err := normalizeVersioningStatus(*statusFlag, *enable, *suspend)
	if err != nil {
		return err
	}

	err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
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
		currentStatus, lockEnabled := getBucketVersioningState(entry)

		if desiredStatus == "" {
			if lockEnabled {
				currentStatus = s3_constants.VersioningEnabled
			}
			fmt.Fprintf(writer, "Bucket: %s\n", *bucketName)
			switch currentStatus {
			case s3_constants.VersioningEnabled:
				fmt.Fprintf(writer, "Versioning: Enabled\n")
			case s3_constants.VersioningSuspended:
				fmt.Fprintf(writer, "Versioning: Suspended\n")
			case "":
				fmt.Fprintf(writer, "Versioning: Unconfigured\n")
			default:
				fmt.Fprintf(writer, "Versioning: %s\n", currentStatus)
			}
			if lockEnabled {
				fmt.Fprintf(writer, "Object Lock: Enabled\n")
			}
			return nil
		}

		if lockEnabled && desiredStatus == s3_constants.VersioningSuspended {
			return fmt.Errorf("cannot suspend versioning on bucket %s: Object Lock is enabled", *bucketName)
		}

		if entry.Extended == nil {
			entry.Extended = make(map[string][]byte)
		}
		entry.Extended[s3_constants.ExtVersioningKey] = []byte(desiredStatus)

		if _, err := client.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{
			Directory: filerBucketsPath,
			Entry:     entry,
		}); err != nil {
			return fmt.Errorf("failed to update bucket: %w", err)
		}

		fmt.Fprintf(writer, "Bucket %s versioning set to %s\n", *bucketName, desiredStatus)
		return nil
	})

	return err
}

func normalizeVersioningStatus(statusFlag string, enable bool, suspend bool) (string, error) {
	if enable && suspend {
		return "", fmt.Errorf("only one of -enable or -suspend can be set")
	}
	if (enable || suspend) && statusFlag != "" {
		return "", fmt.Errorf("use either -status or -enable/-suspend, not both")
	}
	if enable {
		return s3_constants.VersioningEnabled, nil
	}
	if suspend {
		return s3_constants.VersioningSuspended, nil
	}
	if statusFlag == "" {
		return "", nil
	}
	switch strings.ToLower(strings.TrimSpace(statusFlag)) {
	case "enabled":
		return s3_constants.VersioningEnabled, nil
	case "suspended":
		return s3_constants.VersioningSuspended, nil
	default:
		return "", fmt.Errorf("invalid versioning status %q: must be Enabled or Suspended", statusFlag)
	}
}

func getBucketVersioningState(entry *filer_pb.Entry) (status string, lockEnabled bool) {
	if entry.Extended == nil {
		return "", false
	}
	if versioning, ok := entry.Extended[s3_constants.ExtVersioningKey]; ok {
		status = string(versioning)
	}
	if lockStatus, ok := entry.Extended[s3_constants.ExtObjectLockEnabledKey]; ok {
		lockEnabled = string(lockStatus) == s3_constants.ObjectLockEnabled
	}
	return status, lockEnabled
}
