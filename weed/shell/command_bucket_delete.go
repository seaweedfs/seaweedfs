package shell

import (
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

	_, parseErr := commandEnv.parseUrl(findInputDirectory(bucketCommand.Args()))
	if parseErr != nil {
		return parseErr
	}

	var filerBucketsPath string
	filerBucketsPath, err = readFilerBucketsPath(commandEnv)
	if err != nil {
		return fmt.Errorf("read buckets: %v", err)
	}

	return filer_pb.Remove(commandEnv, filerBucketsPath, *bucketName, false, true, true)

}
