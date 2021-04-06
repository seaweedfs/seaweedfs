package shell

import (
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/util"
	"io"
	"math"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func init() {
	Commands = append(Commands, &commandS3CleanUploads{})
}

type commandS3CleanUploads struct {
}

func (c *commandS3CleanUploads) Name() string {
	return "s3.clean.uploads"
}

func (c *commandS3CleanUploads) Help() string {
	return `clean up stale multipart uploads

	Example:
		s3.clean.uploads -replication 001

`
}

func (c *commandS3CleanUploads) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	bucketCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	uploadedTimeAgo := bucketCommand.Duration("timeAgo", 24*time.Hour, "created time before now. \"1.5h\" or \"2h45m\". Valid time units are \"m\", \"h\"")
	if err = bucketCommand.Parse(args); err != nil {
		return nil
	}

	var filerBucketsPath string
	filerBucketsPath, err = readFilerBucketsPath(commandEnv)
	if err != nil {
		return fmt.Errorf("read buckets: %v", err)
	}

	var buckets []string
	err = filer_pb.List(commandEnv, filerBucketsPath, "", func(entry *filer_pb.Entry, isLast bool) error {
		buckets = append(buckets, entry.Name)
		return nil
	}, "", false, math.MaxUint32)
	if err != nil {
		return fmt.Errorf("list buckets under %v: %v", filerBucketsPath, err)
	}

	for _, bucket := range buckets {
		c.cleanupUploads(commandEnv, writer, filerBucketsPath, bucket, *uploadedTimeAgo)
	}

	return err

}

func (c *commandS3CleanUploads) cleanupUploads(commandEnv *CommandEnv, writer io.Writer, filerBucketsPath string, bucket string, timeAgo time.Duration) error {
	uploadsDir := filerBucketsPath + "/" + bucket + "/.uploads"
	var staleUploads []string
	now := time.Now()
	err := filer_pb.List(commandEnv, uploadsDir, "", func(entry *filer_pb.Entry, isLast bool) error {
		ctime := time.Unix(entry.Attributes.Crtime, 0)
		if ctime.Add(timeAgo).Before(now) {
			staleUploads = append(staleUploads, entry.Name)
		}
		return nil
	}, "", false, math.MaxUint32)
	if err != nil {
		return fmt.Errorf("list uploads under %v: %v", uploadsDir, err)
	}

	for _, staleUpload := range staleUploads {
		deleteUrl := fmt.Sprintf("http://%s:%d%s/%s?recursive=true&ignoreRecursiveError=true", commandEnv.option.FilerHost, commandEnv.option.FilerPort, uploadsDir, staleUpload)
		fmt.Fprintf(writer, "purge %s\n", deleteUrl)

		err = util.Delete(deleteUrl, "")
		if err != nil {
			return fmt.Errorf("purge %s/%s: %v", uploadsDir, staleUpload, err)
		}
	}

	return nil

}
