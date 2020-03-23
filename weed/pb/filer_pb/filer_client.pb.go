package filer_pb

import (
	"context"
	"fmt"
	"io"
	"math"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type FilerClient interface {
	WithFilerClient(fn func(SeaweedFilerClient) error) error
	AdjustedUrl(hostAndPort string) string
}

func GetEntry(filerClient FilerClient, fullFilePath util.FullPath) (entry *Entry, err error) {

	dir, name := fullFilePath.DirAndName()

	err = filerClient.WithFilerClient(func(client SeaweedFilerClient) error {

		request := &LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		}

		// glog.V(3).Infof("read %s request: %v", fullFilePath, request)
		resp, err := LookupEntry(client, request)
		if err != nil {
			if err == ErrNotFound {
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

func ReadDirAllEntries(filerClient FilerClient, fullDirPath util.FullPath, prefix string, fn func(entry *Entry, isLast bool)) (err error) {

	err = filerClient.WithFilerClient(func(client SeaweedFilerClient) error {

		lastEntryName := ""

		request := &ListEntriesRequest{
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

		var prevEntry *Entry
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
