package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
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
		s3.bucket.create -name <bucket_name> -replication 001
`
}

func (c *commandS3BucketCreate) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	bucketCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	bucketName := bucketCommand.String("name", "", "bucket name")
	replication := bucketCommand.String("replication", "", "replication setting for the bucket, if not set "+
		"it will honor the value defined by the filer if it exists, "+
		"else it will honor the value defined on the master")
	if err = bucketCommand.Parse(args); err != nil {
		return nil
	}

	if *bucketName == "" {
		return fmt.Errorf("empty bucket name")
	}

	err = commandEnv.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return fmt.Errorf("get filer configuration: %v", err)
		}
		filerBucketsPath := resp.DirBuckets

		println("create bucket under", filerBucketsPath)

		entry := &filer_pb.Entry{
			Name:        *bucketName,
			IsDirectory: true,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:       time.Now().Unix(),
				Crtime:      time.Now().Unix(),
				FileMode:    uint32(0777 | os.ModeDir),
				Collection:  *bucketName,
				Replication: *replication,
			},
		}

		if err := filer_pb.CreateEntry(client, &filer_pb.CreateEntryRequest{
			Directory: filerBucketsPath,
			Entry:     entry,
		}); err != nil {
			return err
		}

		println("created bucket", *bucketName)

		return nil

	})

	return err

}
