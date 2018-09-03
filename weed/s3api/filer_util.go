package s3api

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func (s3a *S3ApiServer) mkdir(parentDirectoryPath string, dirName string) error {
	return s3a.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.CreateEntryRequest{
			Directory: parentDirectoryPath,
			Entry: &filer_pb.Entry{
				Name:        dirName,
				IsDirectory: true,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    time.Now().Unix(),
					Crtime:   time.Now().Unix(),
					FileMode: uint32(0777 | os.ModeDir),
					Uid:      OS_UID,
					Gid:      OS_GID,
				},
			},
		}

		glog.V(1).Infof("create bucket: %v", request)
		if _, err := client.CreateEntry(context.Background(), request); err != nil {
			return fmt.Errorf("mkdir %s/%s: %v", parentDirectoryPath, dirName, err)
		}

		return nil
	})
}

func (s3a *S3ApiServer) list(parentDirectoryPath string) (entries []*filer_pb.Entry, err error) {

	err = s3a.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.ListEntriesRequest{
			Directory: s3a.option.BucketsPath,
		}

		glog.V(4).Infof("read directory: %v", request)
		resp, err := client.ListEntries(context.Background(), request)
		if err != nil {
			return fmt.Errorf("list dir %v: %v", parentDirectoryPath, err)
		}

		entries = resp.Entries

		return nil
	})

	return

}

func (s3a *S3ApiServer) rm(parentDirectoryPath string, entryName string, isDirectory, isDeleteData, isRecursive bool) error {

	return s3a.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		ctx := context.Background()

		request := &filer_pb.DeleteEntryRequest{
			Directory:    parentDirectoryPath,
			Name:         entryName,
			IsDirectory:  isDirectory,
			IsDeleteData: isDeleteData,
			IsRecursive:  isRecursive,
		}

		glog.V(1).Infof("delete entry %v/%v: %v", parentDirectoryPath, entryName, request)
		if _, err := client.DeleteEntry(ctx, request); err != nil {
			return fmt.Errorf("delete entry %s/%s: %v", parentDirectoryPath, entryName, err)
		}

		return nil
	})

}
