package s3api

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"strings"
)

func (s3a *S3ApiServer) mkdir(parentDirectoryPath string, dirName string, fn func(entry *filer_pb.Entry)) error {

	return filer_pb.Mkdir(s3a, parentDirectoryPath, dirName, fn)

}

func (s3a *S3ApiServer) mkFile(parentDirectoryPath string, fileName string, chunks []*filer_pb.FileChunk, fn func(entry *filer_pb.Entry)) error {

	return filer_pb.MkFile(s3a, parentDirectoryPath, fileName, chunks, fn)

}

func (s3a *S3ApiServer) list(parentDirectoryPath, prefix, startFrom string, inclusive bool, limit uint32) (entries []*filer_pb.Entry, isLast bool, err error) {

	err = filer_pb.List(s3a, parentDirectoryPath, prefix, func(entry *filer_pb.Entry, isLastEntry bool) error {
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

	return filer_pb.Exists(s3a, parentDirectoryPath, entryName, isDirectory)

}

func (s3a *S3ApiServer) touch(parentDirectoryPath string, entryName string, entry *filer_pb.Entry) (err error) {

	return filer_pb.Touch(s3a, parentDirectoryPath, entryName, entry)

}

func (s3a *S3ApiServer) getEntry(parentDirectoryPath, entryName string) (entry *filer_pb.Entry, err error) {
	fullPath := util.NewFullPath(parentDirectoryPath, entryName)
	return filer_pb.GetEntry(s3a, fullPath)
}

func (s3a *S3ApiServer) updateEntry(parentDirectoryPath string, newEntry *filer_pb.Entry) error {
	updateEntryRequest := &filer_pb.UpdateEntryRequest{
		Directory: parentDirectoryPath,
		Entry:     newEntry,
	}

	err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		err := filer_pb.UpdateEntry(client, updateEntryRequest)
		if err != nil {
			return err
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
