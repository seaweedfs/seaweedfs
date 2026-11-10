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

	"github.com/google/uuid"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
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

	var filerBucketsPath string
	filerBucketsPath, err = readFilerBucketsPath(commandEnv)
	if err != nil {
		return fmt.Errorf("read buckets: %w", err)
	}

	var buckets []*filer_pb.Entry
	err = filer_pb.List(context.Background(), commandEnv, filerBucketsPath, "", func(entry *filer_pb.Entry, isLast bool) error {
		buckets = append(buckets, entry)
		return nil
	}, "", false, math.MaxUint32)
	if err != nil {
		return fmt.Errorf("list buckets under %v: %w", filerBucketsPath, err)
	}

	lock := newObjectWriteLock(commandEnv)
	for _, bucket := range buckets {
		versioning, lockEnabled := getBucketVersioningState(bucket)
		versioned := versioning == s3_constants.VersioningEnabled || lockEnabled
		if err := c.cleanupUploads(commandEnv, writer, filerBucketsPath, bucket.Name, versioned, *uploadedTimeAgo, lock); err != nil {
			fmt.Fprintf(writer, "failed cleanup uploads for bucket %s: %v", bucket.Name, err)
		}
	}

	return err
}

func (c *commandS3CleanUploads) cleanupUploads(commandEnv *CommandEnv, writer io.Writer, filerBucketsPath string, bucket string, versioned bool, timeAgo time.Duration, lock objectWriteLock) error {
	bucketDir := filerBucketsPath + "/" + bucket
	uploadsDir := bucketDir + "/" + s3_constants.MultipartUploadsFolder
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

	for _, staleUpload := range staleUploads {
		held, release, lockErr := lock(uploadObjectPath(bucketDir, staleUpload))
		if lockErr != nil {
			fmt.Fprintf(writer, "skip %s: %v\n", staleUpload.Name, lockErr)
			continue
		}
		err := c.removeUploadDir(commandEnv, writer, bucketDir, uploadsDir, staleUpload, versioned, held)
		release()
		if err != nil {
			return err
		}
	}

	return nil
}

// objectWriteLock takes the distributed lock the S3 gateway commits a
// completion under when it is not routed to the object's owner filer, and
// renews it until release.
type objectWriteLock func(objectPath string) (held func() bool, release func(), err error)

func newObjectWriteLock(commandEnv *CommandEnv) objectWriteLock {
	lockClient := cluster.NewLockClient(commandEnv.option.GrpcDialOption, commandEnv.FilerAddress())
	owner := fmt.Sprintf("s3.clean.uploads-%d", util.RandomInt32())
	return func(objectPath string) (func() bool, func(), error) {
		if objectPath == "" {
			return func() bool { return true }, func() {}, nil
		}
		key := objectRouteKey(objectPath)
		lock := lockClient.NewBlockingLongLivedLock(key, owner, 15*time.Second)
		if lock == nil {
			return nil, nil, fmt.Errorf("lock %s: no lock server", key)
		}
		return lock.IsLocked, func() {
			if err := lock.Stop(); err != nil {
				glog.Warningf("release lock %s: %v", key, err)
			}
		}, nil
	}
}

// removeUploadDir mirrors the S3 gateway's abort, under the object write lock.
// A completed upload's part entries share chunks with the finished object, so
// only the metadata goes; an open upload's chunks are freed by
// routedUploadDirDelete. An undecidable lookup, or a lost lock, is left for
// the next run.
func (c *commandS3CleanUploads) removeUploadDir(filerClient filer_pb.FilerClient, writer io.Writer, bucketDir, uploadsDir string, upload *filer_pb.Entry, versioned bool, lockHeld func() bool) error {
	completed, err := c.uploadCompleted(filerClient, bucketDir, upload)
	if err != nil {
		fmt.Fprintf(writer, "skip %s: %v\n", upload.Name, err)
		return nil
	}
	if !lockHeld() {
		fmt.Fprintf(writer, "skip %s: lost the object write lock\n", upload.Name)
		return nil
	}
	if !completed {
		freed, err := routedUploadDirDelete(filerClient, uploadObjectPath(bucketDir, upload), uploadCommitPath(bucketDir, upload, versioned), uploadsDir, upload.Name)
		if err != nil {
			return fmt.Errorf("purge %s/%s: %v", uploadsDir, upload.Name, err)
		}
		if freed {
			fmt.Fprintf(writer, "purge %s/%s\n", uploadsDir, upload.Name)
			return nil
		}
	}
	fmt.Fprintf(writer, "purge %s/%s metadata only\n", uploadsDir, upload.Name)
	if err := filer_pb.Remove(context.Background(), filerClient, uploadsDir, upload.Name, false, true, true, false, nil); err != nil {
		return fmt.Errorf("purge %s/%s: %v", uploadsDir, upload.Name, err)
	}
	return nil
}

// routedUploadDirDelete frees an open upload's chunks in an ObjectTransaction
// on the object's owner filer, under the path lock a routed completion
// commits under. It deletes nothing and reports false when the object
// already carries the upload.
func routedUploadDirDelete(filerClient filer_pb.FilerClient, objectPath, commitPath, uploadsDir, uploadId string) (bool, error) {
	if objectPath == "" {
		return true, filer_pb.Remove(context.Background(), filerClient, uploadsDir, uploadId, true, true, true, false, nil)
	}
	var resp *filer_pb.ObjectTransactionResponse
	err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		var txnErr error
		resp, txnErr = client.ObjectTransaction(context.Background(), &filer_pb.ObjectTransactionRequest{
			LockKey:      commitPath,
			RouteKey:     objectRouteKey(objectPath),
			ConditionKey: commitPath,
			Condition: &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{{
				Kind:     filer_pb.WriteCondition_IF_EXTENDED_NOT_EQUAL,
				ExtKey:   s3_constants.SeaweedFSUploadId,
				ExtValue: uploadId,
			}}},
			Mutations: []*filer_pb.ObjectMutation{{
				Type:         filer_pb.ObjectMutation_DELETE,
				Directory:    uploadsDir,
				Name:         uploadId,
				IsDeleteData: true,
				IsRecursive:  true,
			}},
			RequestId: uuid.NewString(),
		})
		return txnErr
	})
	if err != nil {
		return false, err
	}
	if resp.ErrorCode == filer_pb.FilerError_PRECONDITION_FAILED {
		return false, nil
	}
	if resp.Error != "" {
		return false, errors.New(resp.Error)
	}
	return true, nil
}

// uploadObjectPath is the filer path of the object the upload completes into, as
// the gateway's toFilerPath derives it, or "" when the upload names none.
func uploadObjectPath(bucketDir string, upload *filer_pb.Entry) string {
	objectKey := string(upload.Extended[s3_constants.ExtMultipartObjectKey])
	if objectKey == "" {
		return ""
	}
	return bucketDir + "/" + s3_constants.NormalizeObjectKey(objectKey)
}

// uploadCommitPath is the path a routed completion of the upload locks: the
// object path in a versioned bucket, else the entry it writes, which for a
// trailing-slash key lies inside the directory the key names.
func uploadCommitPath(bucketDir string, upload *filer_pb.Entry, versioned bool) string {
	objectKey := string(upload.Extended[s3_constants.ExtMultipartObjectKey])
	if objectKey == "" || versioned {
		return uploadObjectPath(bucketDir, upload)
	}
	dir := path.Dir(objectKey)
	if dir == "." {
		dir = ""
	}
	return path.Join(bucketDir, dir, path.Base(objectKey))
}

func objectRouteKey(objectPath string) string {
	return s3_constants.ObjectWriteRouteKeyPrefix + strings.TrimSuffix(objectPath, "/")
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
