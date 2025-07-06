package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func init() {
	Commands = append(Commands, &commandS3BucketList{})
}

type commandS3BucketList struct {
}

func (c *commandS3BucketList) Name() string {
	return "s3.bucket.list"
}

func (c *commandS3BucketList) Help() string {
	return `list all buckets

`
}

func (c *commandS3BucketList) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3BucketList) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	bucketCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	if err = bucketCommand.Parse(args); err != nil {
		return nil
	}

	// collect collection information
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}
	collectionInfos := make(map[string]*CollectionInfo)
	collectCollectionInfo(topologyInfo, collectionInfos)

	_, parseErr := commandEnv.parseUrl(findInputDirectory(bucketCommand.Args()))
	if parseErr != nil {
		return parseErr
	}

	var filerBucketsPath string
	filerBucketsPath, err = readFilerBucketsPath(commandEnv)
	if err != nil {
		return fmt.Errorf("read buckets: %v", err)
	}

	err = filer_pb.List(context.Background(), commandEnv, filerBucketsPath, "", func(entry *filer_pb.Entry, isLast bool) error {
		if !entry.IsDirectory {
			return nil
		}
		collection := getCollectionName(commandEnv, entry.Name)
		var collectionSize, fileCount float64
		if collectionInfo, found := collectionInfos[collection]; found {
			collectionSize = collectionInfo.Size
			fileCount = collectionInfo.FileCount - collectionInfo.DeleteCount
		}
		fmt.Fprintf(writer, "  %s\tsize:%.0f\tchunk:%.0f", entry.Name, collectionSize, fileCount)
		if entry.Quota > 0 {
			fmt.Fprintf(writer, "\tquota:%d\tusage:%.2f%%", entry.Quota, float64(collectionSize)*100/float64(entry.Quota))
		}
		fmt.Fprintln(writer)
		return nil
	}, "", false, math.MaxUint32)
	if err != nil {
		return fmt.Errorf("list buckets under %v: %v", filerBucketsPath, err)
	}

	return err

}

func readFilerBucketsPath(filerClient filer_pb.FilerClient) (filerBucketsPath string, err error) {
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return fmt.Errorf("get filer configuration: %v", err)
		}
		filerBucketsPath = resp.DirBuckets

		return nil

	})

	return filerBucketsPath, err
}
