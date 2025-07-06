package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

func init() {
	Commands = append(Commands, &commandS3CleanUploads{})
}

type commandS3CleanUploads struct{}

func (c *commandS3CleanUploads) Name() string {
	return "s3.clean.uploads"
}

func (c *commandS3CleanUploads) Help() string {
	return `clean up stale multipart uploads

	Example:
		s3.clean.uploads -timeAgo 1.5h

`
}

func (c *commandS3CleanUploads) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3CleanUploads) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	bucketCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	uploadedTimeAgo := bucketCommand.Duration("timeAgo", 24*time.Hour, "created time before now. \"1.5h\" or \"2h45m\". Valid time units are \"m\", \"h\"")
	if err = bucketCommand.Parse(args); err != nil {
		return nil
	}

	signingKey := util.GetViper().GetString("jwt.filer_signing.key")

	var filerBucketsPath string
	filerBucketsPath, err = readFilerBucketsPath(commandEnv)
	if err != nil {
		return fmt.Errorf("read buckets: %v", err)
	}

	var buckets []string
	err = filer_pb.List(context.Background(), commandEnv, filerBucketsPath, "", func(entry *filer_pb.Entry, isLast bool) error {
		buckets = append(buckets, entry.Name)
		return nil
	}, "", false, math.MaxUint32)
	if err != nil {
		return fmt.Errorf("list buckets under %v: %v", filerBucketsPath, err)
	}

	for _, bucket := range buckets {
		if err := c.cleanupUploads(commandEnv, writer, filerBucketsPath, bucket, *uploadedTimeAgo, signingKey); err != nil {
			fmt.Fprintf(writer, "failed cleanup uploads for bucket %s: %v", bucket, err)
		}
	}

	return err
}

func (c *commandS3CleanUploads) cleanupUploads(commandEnv *CommandEnv, writer io.Writer, filerBucketsPath string, bucket string, timeAgo time.Duration, signingKey string) error {
	uploadsDir := filerBucketsPath + "/" + bucket + "/" + s3_constants.MultipartUploadsFolder
	var staleUploads []string
	now := time.Now()
	err := filer_pb.List(context.Background(), commandEnv, uploadsDir, "", func(entry *filer_pb.Entry, isLast bool) error {
		ctime := time.Unix(entry.Attributes.Crtime, 0)
		if ctime.Add(timeAgo).Before(now) {
			staleUploads = append(staleUploads, entry.Name)
		}
		return nil
	}, "", false, math.MaxUint32)
	if err != nil {
		return fmt.Errorf("list uploads under %v: %v", uploadsDir, err)
	}

	var encodedJwt security.EncodedJwt
	if signingKey != "" {
		encodedJwt = security.GenJwtForFilerServer(security.SigningKey(signingKey), 15*60)
	}

	for _, staleUpload := range staleUploads {
		deleteUrl := fmt.Sprintf("http://%s%s/%s?recursive=true&ignoreRecursiveError=true", commandEnv.option.FilerAddress.ToHttpAddress(), uploadsDir, staleUpload)
		fmt.Fprintf(writer, "purge %s\n", deleteUrl)

		err = util_http.Delete(deleteUrl, string(encodedJwt))
		if err != nil && err.Error() != "" {
			return fmt.Errorf("purge %s/%s: %v", uploadsDir, staleUpload, err)
		}
	}

	return nil
}
