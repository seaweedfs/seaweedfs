package shell

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_objectlock"
)

func init() {
	Commands = append(Commands, &commandS3BucketDelete{})
}

type commandS3BucketDelete struct {
}

func (c *commandS3BucketDelete) Name() string {
	return "s3.bucket.delete"
}

func (c *commandS3BucketDelete) Help() string {
	return `delete a bucket by a given name

	s3.bucket.delete -name <bucket_name>
`
}

func (c *commandS3BucketDelete) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3BucketDelete) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	bucketCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	bucketName := bucketCommand.String("name", "", "bucket name")
	if err = bucketCommand.Parse(args); err != nil {
		return nil
	}

	if *bucketName == "" {
		return fmt.Errorf("empty bucket name")
	}

	_, parseErr := commandEnv.parseUrl(findInputDirectory(bucketCommand.Args()))
	if parseErr != nil {
		return parseErr
	}

	var filerBucketsPath string
	filerBucketsPath, err = readFilerBucketsPath(commandEnv)
	if err != nil {
		return fmt.Errorf("read buckets: %w", err)
	}

	// Check if bucket has Object Lock enabled and if there are locked objects
	err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return s3_objectlock.CheckBucketForLockedObjects(client, filerBucketsPath, *bucketName)
	})
	if err != nil {
		return err
	}

	// delete the collection directly first
	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		_, err = client.CollectionDelete(context.Background(), &master_pb.CollectionDeleteRequest{
			Name: getCollectionName(commandEnv, *bucketName),
		})
		return err
	})
	if err != nil {
		return
	}

	return filer_pb.Remove(context.Background(), commandEnv, filerBucketsPath, *bucketName, false, true, true, false, nil)

}
