package filer_pb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var (
	OS_UID = uint32(os.Getuid())
	OS_GID = uint32(os.Getgid())
)

type FilerClient interface {
	WithFilerClient(fn func(SeaweedFilerClient) error) error
	AdjustedUrl(location *Location) string
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

type EachEntryFunciton func(entry *Entry, isLast bool) error

func ReadDirAllEntries(filerClient FilerClient, fullDirPath util.FullPath, prefix string, fn EachEntryFunciton) (err error) {

	var counter uint32
	var startFrom string
	var counterFunc = func(entry *Entry, isLast bool) error {
		counter++
		startFrom = entry.Name
		return fn(entry, isLast)
	}

	var paginationLimit uint32 = 10000

	if err = doList(filerClient, fullDirPath, prefix, counterFunc, "", false, paginationLimit); err != nil {
		return err
	}

	for counter == paginationLimit {
		counter = 0
		if err = doList(filerClient, fullDirPath, prefix, counterFunc, startFrom, false, paginationLimit); err != nil {
			return err
		}
	}

	return nil
}

func List(filerClient FilerClient, parentDirectoryPath, prefix string, fn EachEntryFunciton, startFrom string, inclusive bool, limit uint32) (err error) {
	return doList(filerClient, util.FullPath(parentDirectoryPath), prefix, fn, startFrom, inclusive, limit)
}

func doList(filerClient FilerClient, fullDirPath util.FullPath, prefix string, fn EachEntryFunciton, startFrom string, inclusive bool, limit uint32) (err error) {

	err = filerClient.WithFilerClient(func(client SeaweedFilerClient) error {

		request := &ListEntriesRequest{
			Directory:          string(fullDirPath),
			Prefix:             prefix,
			StartFromFileName:  startFrom,
			Limit:              limit,
			InclusiveStartFrom: inclusive,
		}

		glog.V(4).Infof("read directory: %v", request)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stream, err := client.ListEntries(ctx, request)
		if err != nil {
			return fmt.Errorf("list %s: %v", fullDirPath, err)
		}

		var prevEntry *Entry
		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr == io.EOF {
					if prevEntry != nil {
						if err := fn(prevEntry, true); err != nil {
							return err
						}
					}
					break
				} else {
					return recvErr
				}
			}
			if prevEntry != nil {
				if err := fn(prevEntry, false); err != nil {
					return err
				}
			}
			prevEntry = resp.Entry
		}

		return nil

	})

	return
}

func Exists(filerClient FilerClient, parentDirectoryPath string, entryName string, isDirectory bool) (exists bool, err error) {

	err = filerClient.WithFilerClient(func(client SeaweedFilerClient) error {

		request := &LookupDirectoryEntryRequest{
			Directory: parentDirectoryPath,
			Name:      entryName,
		}

		glog.V(4).Infof("exists entry %v/%v: %v", parentDirectoryPath, entryName, request)
		resp, err := LookupEntry(client, request)
		if err != nil {
			if err == ErrNotFound {
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

func Mkdir(filerClient FilerClient, parentDirectoryPath string, dirName string, fn func(entry *Entry)) error {
	return filerClient.WithFilerClient(func(client SeaweedFilerClient) error {

		entry := &Entry{
			Name:        dirName,
			IsDirectory: true,
			Attributes: &FuseAttributes{
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

		request := &CreateEntryRequest{
			Directory: parentDirectoryPath,
			Entry:     entry,
		}

		glog.V(1).Infof("mkdir: %v", request)
		if err := CreateEntry(client, request); err != nil {
			glog.V(0).Infof("mkdir %v: %v", request, err)
			return fmt.Errorf("mkdir %s/%s: %v", parentDirectoryPath, dirName, err)
		}

		return nil
	})
}

func MkFile(filerClient FilerClient, parentDirectoryPath string, fileName string, chunks []*FileChunk) error {
	return filerClient.WithFilerClient(func(client SeaweedFilerClient) error {

		entry := &Entry{
			Name:        fileName,
			IsDirectory: false,
			Attributes: &FuseAttributes{
				Mtime:    time.Now().Unix(),
				Crtime:   time.Now().Unix(),
				FileMode: uint32(0770),
				Uid:      OS_UID,
				Gid:      OS_GID,
			},
			Chunks: chunks,
		}

		request := &CreateEntryRequest{
			Directory: parentDirectoryPath,
			Entry:     entry,
		}

		glog.V(1).Infof("create file: %s/%s", parentDirectoryPath, fileName)
		if err := CreateEntry(client, request); err != nil {
			glog.V(0).Infof("create file %v:%v", request, err)
			return fmt.Errorf("create file %s/%s: %v", parentDirectoryPath, fileName, err)
		}

		return nil
	})
}

func Remove(filerClient FilerClient, parentDirectoryPath, name string, isDeleteData, isRecursive, ignoreRecursiveErr, isFromOtherCluster bool, signatures []int32) error {
	return filerClient.WithFilerClient(func(client SeaweedFilerClient) error {

		deleteEntryRequest := &DeleteEntryRequest{
			Directory:            parentDirectoryPath,
			Name:                 name,
			IsDeleteData:         isDeleteData,
			IsRecursive:          isRecursive,
			IgnoreRecursiveError: ignoreRecursiveErr,
			IsFromOtherCluster:   isFromOtherCluster,
			Signatures:           signatures,
		}
		if resp, err := client.DeleteEntry(context.Background(), deleteEntryRequest); err != nil {
			if strings.Contains(err.Error(), ErrNotFound.Error()) {
				return nil
			}
			return err
		} else {
			if resp.Error != "" {
				if strings.Contains(resp.Error, ErrNotFound.Error()) {
					return nil
				}
				return errors.New(resp.Error)
			}
		}

		return nil

	})
}
