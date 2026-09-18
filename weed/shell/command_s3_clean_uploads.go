package shell

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"path"
	"strings"
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
		return fmt.Errorf("read buckets: %w", err)
	}

	var buckets []string
	err = filer_pb.List(context.Background(), commandEnv, filerBucketsPath, "", func(entry *filer_pb.Entry, isLast bool) error {
		buckets = append(buckets, entry.Name)
		return nil
	}, "", false, math.MaxUint32)
	if err != nil {
		return fmt.Errorf("list buckets under %v: %w", filerBucketsPath, err)
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
	var staleUploads []*filer_pb.Entry
	now := time.Now()
	err := filer_pb.List(context.Background(), commandEnv, uploadsDir, "", func(entry *filer_pb.Entry, isLast bool) error {
		ctime := time.Unix(entry.Attributes.Crtime, 0)
		if ctime.Add(timeAgo).Before(now) {
			staleUploads = append(staleUploads, entry)
		}
		return nil
	}, "", false, math.MaxUint32)
	if err != nil {
		return fmt.Errorf("list uploads under %v: %w", uploadsDir, err)
	}

	var encodedJwt security.EncodedJwt
	if signingKey != "" {
		encodedJwt = security.GenJwtForFilerServer(security.SigningKey(signingKey), 15*60)
	}

	for _, staleUpload := range staleUploads {
		// A completed upload's part entries share chunks with the finished
		// object, so purging their data corrupts it. Completion normally
		// removes this directory itself; a survivor means that cleanup failed
		// and only the metadata should go. An undecidable lookup is left for
		// the next run rather than risk live chunks.
		completed, checkErr := c.uploadCompleted(commandEnv, filerBucketsPath+"/"+bucket, staleUpload)
		if checkErr != nil {
			fmt.Fprintf(writer, "skip %s: %v\n", staleUpload.Name, checkErr)
			continue
		}
		deleteUrl := fmt.Sprintf("http://%s%s/%s?recursive=true&ignoreRecursiveError=true", commandEnv.option.FilerAddress.ToHttpAddress(), uploadsDir, staleUpload.Name)
		if completed {
			deleteUrl += "&skipChunkDeletion=true"
		}
		fmt.Fprintf(writer, "purge %s\n", deleteUrl)

		err = util_http.Delete(deleteUrl, string(encodedJwt))
		if err != nil && err.Error() != "" {
			return fmt.Errorf("purge %s/%s: %v", uploadsDir, staleUpload.Name, err)
		}
	}

	return nil
}

// uploadCompleted reports whether the upload assembled into an object: the
// object entry, or any version file under <key>.versions, still carries the
// upload id completion stamps on it.
func (c *commandS3CleanUploads) uploadCompleted(filerClient filer_pb.FilerClient, bucketDir string, upload *filer_pb.Entry) (bool, error) {
	objectKey := string(upload.Extended[s3_constants.ExtMultipartObjectKey])
	if objectKey == "" {
		return false, nil
	}
	// Derive the object location the same way completion's getEntryNameAndDir
	// does: a trailing-slash key stores the object inside the directory it
	// names, so FullPath+DirAndName would look one level too high.
	name := path.Base(objectKey)
	dir := path.Dir(objectKey)
	if dir == "." {
		dir = ""
	}
	objectDir := util.FullPath(bucketDir + "/" + dir)

	completed := false
	err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := filer_pb.LookupEntry(context.Background(), client, &filer_pb.LookupDirectoryEntryRequest{Directory: string(objectDir), Name: name})
		if errors.Is(err, filer_pb.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if resp.Entry != nil && string(resp.Entry.Extended[s3_constants.SeaweedFSUploadId]) == upload.Name {
			completed = true
		}
		return nil
	})
	if err != nil || completed {
		return completed, err
	}

	err = filer_pb.List(context.Background(), filerClient, string(objectDir)+"/"+name+s3_constants.VersionsFolder, "", func(entry *filer_pb.Entry, isLast bool) error {
		if string(entry.Extended[s3_constants.SeaweedFSUploadId]) == upload.Name {
			completed = true
		}
		return nil
	}, "", false, math.MaxUint32)
	if err != nil && (errors.Is(err, filer_pb.ErrNotFound) || strings.Contains(err.Error(), filer_pb.ErrNotFound.Error())) {
		return false, nil
	}
	return completed, err
}
