package filer2

import (
	"context"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func VolumeId(fileId string) string {
	lastCommaIndex := strings.LastIndex(fileId, ",")
	if lastCommaIndex > 0 {
		return fileId[:lastCommaIndex]
	}
	return fileId
}

type FilerClient interface {
	WithFilerClient(fn func(filer_pb.SeaweedFilerClient) error) error
	AdjustedUrl(hostAndPort string) string
}

func GetEntry(filerClient FilerClient, fullFilePath FullPath) (entry *filer_pb.Entry, err error) {

	dir, name := fullFilePath.DirAndName()

	err = filerClient.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		}

		// glog.V(3).Infof("read %s request: %v", fullFilePath, request)
		resp, err := filer_pb.LookupEntry(client, request)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				return nil
			}
			glog.V(3).Infof("read %s %v: %v", fullFilePath, resp, err)
			return err
		}

		if resp.Entry == nil {
			// glog.V(3).Infof("read %s entry: %v", fullFilePath, entry)
			return nil
		}

		entry = resp.Entry
		return nil
	})

	return
}

func ReadDirAllEntries(filerClient FilerClient, fullDirPath FullPath, prefix string, fn func(entry *filer_pb.Entry, isLast bool)) (err error) {

	err = filerClient.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		lastEntryName := ""

		request := &filer_pb.ListEntriesRequest{
			Directory:         string(fullDirPath),
			Prefix:            prefix,
			StartFromFileName: lastEntryName,
			Limit:             math.MaxUint32,
		}

		glog.V(3).Infof("read directory: %v", request)
		stream, err := client.ListEntries(context.Background(), request)
		if err != nil {
			return fmt.Errorf("list %s: %v", fullDirPath, err)
		}

		var prevEntry *filer_pb.Entry
		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr == io.EOF {
					if prevEntry != nil {
						fn(prevEntry, true)
					}
					break
				} else {
					return recvErr
				}
			}
			if prevEntry != nil {
				fn(prevEntry, false)
			}
			prevEntry = resp.Entry
		}

		return nil

	})

	return
}
