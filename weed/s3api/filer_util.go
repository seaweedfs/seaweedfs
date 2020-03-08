package s3api

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func (s3a *S3ApiServer) mkdir(parentDirectoryPath string, dirName string, fn func(entry *filer_pb.Entry)) error {
	return s3a.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		entry := &filer_pb.Entry{
			Name:        dirName,
			IsDirectory: true,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    time.Now().Unix(),
				Crtime:   time.Now().Unix(),
				FileMode: uint32(0777 | os.ModeDir),
				Uid:      OS_UID,
				Gid:      OS_GID,
			},
		}

		if fn != nil {
			fn(entry)
		}

		request := &filer_pb.CreateEntryRequest{
			Directory: parentDirectoryPath,
			Entry:     entry,
		}

		glog.V(1).Infof("mkdir: %v", request)
		if err := filer_pb.CreateEntry(client, request); err != nil {
			glog.V(0).Infof("mkdir %v: %v", request, err)
			return fmt.Errorf("mkdir %s/%s: %v", parentDirectoryPath, dirName, err)
		}

		return nil
	})
}

func (s3a *S3ApiServer) mkFile(parentDirectoryPath string, fileName string, chunks []*filer_pb.FileChunk) error {
	return s3a.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		entry := &filer_pb.Entry{
			Name:        fileName,
			IsDirectory: false,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    time.Now().Unix(),
				Crtime:   time.Now().Unix(),
				FileMode: uint32(0770),
				Uid:      OS_UID,
				Gid:      OS_GID,
			},
			Chunks: chunks,
		}

		request := &filer_pb.CreateEntryRequest{
			Directory: parentDirectoryPath,
			Entry:     entry,
		}

		glog.V(1).Infof("create file: %s/%s", parentDirectoryPath, fileName)
		if err := filer_pb.CreateEntry(client, request); err != nil {
			glog.V(0).Infof("create file %v:%v", request, err)
			return fmt.Errorf("create file %s/%s: %v", parentDirectoryPath, fileName, err)
		}

		return nil
	})
}

func (s3a *S3ApiServer) list(parentDirectoryPath, prefix, startFrom string, inclusive bool, limit int) (entries []*filer_pb.Entry, err error) {

	err = s3a.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.ListEntriesRequest{
			Directory:          parentDirectoryPath,
			Prefix:             prefix,
			StartFromFileName:  startFrom,
			InclusiveStartFrom: inclusive,
			Limit:              uint32(limit),
		}

		glog.V(4).Infof("read directory: %v", request)
		stream, err := client.ListEntries(context.Background(), request)
		if err != nil {
			glog.V(0).Infof("read directory %v: %v", request, err)
			return fmt.Errorf("list dir %v: %v", parentDirectoryPath, err)
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

			entries = append(entries, resp.Entry)

		}

		return nil
	})

	return

}

func (s3a *S3ApiServer) rm(parentDirectoryPath, entryName string, isDirectory, isDeleteData, isRecursive bool) error {

	return s3a.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.DeleteEntryRequest{
			Directory:    parentDirectoryPath,
			Name:         entryName,
			IsDeleteData: isDeleteData,
			IsRecursive:  isRecursive,
		}

		glog.V(1).Infof("delete entry %v/%v: %v", parentDirectoryPath, entryName, request)
		if _, err := client.DeleteEntry(context.Background(), request); err != nil {
			glog.V(0).Infof("delete entry %v: %v", request, err)
			return fmt.Errorf("delete entry %s/%s: %v", parentDirectoryPath, entryName, err)
		}

		return nil
	})

}

func (s3a *S3ApiServer) streamRemove(quiet bool, fn func() (finished bool, parentDirectoryPath string, entryName string, isDeleteData, isRecursive bool), respFn func(err string)) error {

	return s3a.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		stream, err := client.StreamDeleteEntries(context.Background())
		if err != nil {
			glog.V(0).Infof("stream delete entry: %v", err)
			return fmt.Errorf("stream delete entry: %v", err)
		}

		waitc := make(chan struct{})
		go func() {
			for {
				resp, err := stream.Recv()
				if err == io.EOF {
					// read done.
					close(waitc)
					return
				}
				if err != nil {
					glog.V(0).Infof("streamRemove: %v", err)
					return
				}
				respFn(resp.Error)
			}
		}()

		for {
			finished, parentDirectoryPath, entryName, isDeleteData, isRecursive := fn()
			if finished {
				break
			}
			err = stream.Send(&filer_pb.DeleteEntryRequest{
				Directory:            parentDirectoryPath,
				Name:                 entryName,
				IsDeleteData:         isDeleteData,
				IsRecursive:          isRecursive,
				IgnoreRecursiveError: quiet,
			})
			if err != nil {
				glog.V(0).Infof("streamRemove: %v", err)
				break
			}

		}
		stream.CloseSend()
		<-waitc
		return err

	})

}

func (s3a *S3ApiServer) exists(parentDirectoryPath string, entryName string, isDirectory bool) (exists bool, err error) {

	err = s3a.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: parentDirectoryPath,
			Name:      entryName,
		}

		glog.V(4).Infof("exists entry %v/%v: %v", parentDirectoryPath, entryName, request)
		resp, err := filer_pb.LookupEntry(client, request)
		if err != nil {
			if err == filer2.ErrNotFound {
				exists = false
				return nil
			}
			glog.V(0).Infof("exists entry %v: %v", request, err)
			return fmt.Errorf("exists entry %s/%s: %v", parentDirectoryPath, entryName, err)
		}

		exists = resp.Entry.IsDirectory == isDirectory

		return nil
	})

	return
}

func objectKey(key *string) *string {
	if strings.HasPrefix(*key, "/") {
		t := (*key)[1:]
		return &t
	}
	return key
}
