package s3api

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"math"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (s3a *S3ApiServer) mkdir(parentDirectoryPath string, dirName string, fn func(entry *filer_pb.Entry)) error {

	return filer_pb.Mkdir(context.Background(), s3a, parentDirectoryPath, dirName, fn)

}

func (s3a *S3ApiServer) mkFile(parentDirectoryPath string, fileName string, chunks []*filer_pb.FileChunk, fn func(entry *filer_pb.Entry)) error {

	return filer_pb.MkFile(context.Background(), s3a, parentDirectoryPath, fileName, chunks, fn)

}

func (s3a *S3ApiServer) list(parentDirectoryPath, prefix, startFrom string, inclusive bool, limit uint32) (entries []*filer_pb.Entry, isLast bool, err error) {

	err = filer_pb.List(context.Background(), s3a, parentDirectoryPath, prefix, func(entry *filer_pb.Entry, isLastEntry bool) error {
		entries = append(entries, entry)
		if isLastEntry {
			isLast = true
		}
		return nil
	}, startFrom, inclusive, limit)

	if len(entries) == 0 {
		isLast = true
	}

	return

}

func (s3a *S3ApiServer) rm(parentDirectoryPath, entryName string, isDeleteData, isRecursive bool) error {

	return s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		err := doDeleteEntry(client, parentDirectoryPath, entryName, isDeleteData, isRecursive)
		if err != nil {
			return err
		}

		return nil
	})

}

func doDeleteEntry(client filer_pb.SeaweedFilerClient, parentDirectoryPath string, entryName string, isDeleteData bool, isRecursive bool) error {
	request := &filer_pb.DeleteEntryRequest{
		Directory:            parentDirectoryPath,
		Name:                 entryName,
		IsDeleteData:         isDeleteData,
		IsRecursive:          isRecursive,
		IgnoreRecursiveError: true,
	}

	glog.V(1).Infof("delete entry %v/%v: %v", parentDirectoryPath, entryName, request)
	if resp, err := client.DeleteEntry(context.Background(), request); err != nil {
		glog.V(0).Infof("delete entry %v: %v", request, err)
		return fmt.Errorf("delete entry %s/%s: %v", parentDirectoryPath, entryName, err)
	} else {
		if resp.Error != "" {
			return fmt.Errorf("delete entry %s/%s: %v", parentDirectoryPath, entryName, resp.Error)
		}
	}
	return nil
}

func (s3a *S3ApiServer) exists(parentDirectoryPath string, entryName string, isDirectory bool) (exists bool, err error) {

	return filer_pb.Exists(context.Background(), s3a, parentDirectoryPath, entryName, isDirectory)

}

func (s3a *S3ApiServer) touch(parentDirectoryPath string, entryName string, entry *filer_pb.Entry) (err error) {

	return filer_pb.Touch(context.Background(), s3a, parentDirectoryPath, entryName, entry)

}

func (s3a *S3ApiServer) getEntry(parentDirectoryPath, entryName string) (entry *filer_pb.Entry, err error) {
	fullPath := util.NewFullPath(parentDirectoryPath, entryName)
	return filer_pb.GetEntry(context.Background(), s3a, fullPath)
}

func (s3a *S3ApiServer) updateEntry(parentDirectoryPath string, newEntry *filer_pb.Entry) error {
	updateEntryRequest := &filer_pb.UpdateEntryRequest{
		Directory: parentDirectoryPath,
		Entry:     newEntry,
	}

	err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		err := filer_pb.UpdateEntry(context.Background(), client, updateEntryRequest)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

func (s3a *S3ApiServer) updateEntriesTTL(parentDirectoryPath string, ttlSec int32) error {
	var updateErrors []error
	err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		ctx := context.Background()
		_ = filer_pb.SeaweedList(ctx, client, parentDirectoryPath, "", func(entry *filer_pb.Entry, isLast bool) error {
			if entry.IsDirectory {
				if err := s3a.updateEntriesTTL(fmt.Sprintf("%s/%s", strings.TrimRight(parentDirectoryPath, "/"), entry.Name), ttlSec); err != nil {
					updateErrors = append(updateErrors, fmt.Errorf("dir %s: %w", entry.Name, err))
				}
				return nil
			}
			if entry.Attributes == nil {
				entry.Attributes = &filer_pb.FuseAttributes{}
			}
			if entry.Extended == nil {
				entry.Extended = make(map[string][]byte)
			}
			entry.Extended[s3_constants.SeaweedFSExpiresS3] = []byte("true")
			if entry.Attributes.TtlSec == ttlSec {
				return nil
			}
			entry.Attributes.TtlSec = ttlSec
			if err := filer_pb.UpdateEntry(ctx, client, &filer_pb.UpdateEntryRequest{
				Directory: parentDirectoryPath,
				Entry:     entry,
			}); err != nil {
				updateErrors = append(updateErrors, fmt.Errorf("file %s: %w", entry.Name, err))
			}
			return nil
		}, "", false, math.MaxInt32)
		if len(updateErrors) > 0 {
			return fmt.Errorf("failed to update %d entries: %v", len(updateErrors), updateErrors[0])
		}
		return nil
	})
	return err
}

func (s3a *S3ApiServer) getCollectionName(bucket string) string {
	if s3a.option.FilerGroup != "" {
		return fmt.Sprintf("%s_%s", s3a.option.FilerGroup, bucket)
	}
	return bucket
}

func objectKey(key *string) *string {
	if strings.HasPrefix(*key, "/") {
		t := (*key)[1:]
		return &t
	}
	return key
}
