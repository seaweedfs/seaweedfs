package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func init() {
	Commands = append(Commands, &commandBucketList{})
}

type commandBucketList struct {
}

func (c *commandBucketList) Name() string {
	return "bucket.list"
}

func (c *commandBucketList) Help() string {
	return `list all buckets

`
}

func (c *commandBucketList) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	bucketCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	if err = bucketCommand.Parse(args); err != nil {
		return nil
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

		stream, err := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
			Directory: filerBucketsPath,
			Limit:     math.MaxUint32,
		})
		if err != nil {
			return fmt.Errorf("list buckets under %v: %v", filerBucketsPath, err)
		}

		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr == io.EOF {
					break
				} else {
					return recvErr
				}
			}

			if resp.Entry.Attributes.Replication == "" || resp.Entry.Attributes.Replication == "000" {
				fmt.Fprintf(writer, "  %s\n", resp.Entry.Name)
			} else {
				fmt.Fprintf(writer, "  %s\t\t\treplication: %s\n", resp.Entry.Name, resp.Entry.Attributes.Replication)
			}
		}

		return nil

	})

	return err

}
