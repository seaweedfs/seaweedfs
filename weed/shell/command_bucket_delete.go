package shell

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func init() {
	Commands = append(Commands, &commandBucketDelete{})
}

type commandBucketDelete struct {
}

func (c *commandBucketDelete) Name() string {
	return "bucket.delete"
}

func (c *commandBucketDelete) Help() string {
	return `delete a bucket by a given name

	bucket.delete -name <bucket_name>
`
}

func (c *commandBucketDelete) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	bucketCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	bucketName := bucketCommand.String("name", "", "bucket name")
	if err = bucketCommand.Parse(args); err != nil {
		return nil
	}

	if *bucketName == "" {
		return fmt.Errorf("empty bucket name")
	}

	filerServer, filerPort, _, parseErr := commandEnv.parseUrl(findInputDirectory(bucketCommand.Args()))
	if parseErr != nil {
		return parseErr
	}

	ctx := context.Background()

	err = commandEnv.withFilerClient(ctx, filerServer, filerPort, func(ctx context.Context, client filer_pb.SeaweedFilerClient) error {

		resp, err := client.GetFilerConfiguration(ctx, &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return fmt.Errorf("get filer %s:%d configuration: %v", filerServer, filerPort, err)
		}
		filerBucketsPath := resp.DirBuckets

		if _, err := client.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
			Directory:            filerBucketsPath,
			Name:                 *bucketName,
			IsDeleteData:         false,
			IsRecursive:          true,
			IgnoreRecursiveError: true,
		}); err != nil {
			return err
		}

		return nil

	})

	return err

}
