package filer_pb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	OS_UID = uint32(os.Getuid())
	OS_GID = uint32(os.Getgid())
)

type FilerClient interface {
	WithFilerClient(streamingMode bool, fn func(SeaweedFilerClient) error) error // 15 implementation
	AdjustedUrl(location *Location) string
	GetDataCenter() string
}

func GetEntry(ctx context.Context, filerClient FilerClient, fullFilePath util.FullPath) (entry *Entry, err error) {

	dir, name := fullFilePath.DirAndName()

	err = filerClient.WithFilerClient(false, func(client SeaweedFilerClient) error {

		request := &LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		}

		// glog.V(3).Infof("read %s request: %v", fullFilePath, request)
		resp, err := LookupEntry(ctx, client, request)
		if err != nil {
			glog.V(3).InfofCtx(ctx, "read %s %v: %v", fullFilePath, resp, err)
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

type EachEntryFunction func(entry *Entry, isLast bool) error

func ReadDirAllEntries(ctx context.Context, filerClient FilerClient, fullDirPath util.FullPath, prefix string, fn EachEntryFunction) (err error) {
	_, err = ReadDirAllEntriesWithSnapshot(ctx, filerClient, fullDirPath, prefix, fn)
	return err
}

func ReadDirAllEntriesWithSnapshot(ctx context.Context, filerClient FilerClient, fullDirPath util.FullPath, prefix string, fn EachEntryFunction) (snapshotTsNs int64, err error) {
	var counter uint32
	var startFrom string
	var counterFunc = func(entry *Entry, isLast bool) error {
		counter++
		startFrom = entry.Name
		return fn(entry, isLast)
	}

	var paginationLimit uint32 = 10000

	if snapshotTsNs, err = doListWithSnapshot(ctx, filerClient, fullDirPath, prefix, counterFunc, "", false, paginationLimit, 0); err != nil {
		return snapshotTsNs, err
	}

	for counter == paginationLimit {
		counter = 0
		if _, err = doListWithSnapshot(ctx, filerClient, fullDirPath, prefix, counterFunc, startFrom, false, paginationLimit, snapshotTsNs); err != nil {
			return snapshotTsNs, err
		}
	}

	return snapshotTsNs, nil
}

func List(ctx context.Context, filerClient FilerClient, parentDirectoryPath, prefix string, fn EachEntryFunction, startFrom string, inclusive bool, limit uint32) (err error) {
	return filerClient.WithFilerClient(false, func(client SeaweedFilerClient) error {
		return doSeaweedList(ctx, client, util.FullPath(parentDirectoryPath), prefix, fn, startFrom, inclusive, limit)
	})
}

func doListWithSnapshot(ctx context.Context, filerClient FilerClient, fullDirPath util.FullPath, prefix string, fn EachEntryFunction, startFrom string, inclusive bool, limit uint32, snapshotTsNs int64) (actualSnapshotTsNs int64, err error) {
	err = filerClient.WithFilerClient(false, func(client SeaweedFilerClient) error {
		actualSnapshotTsNs, err = DoSeaweedListWithSnapshot(ctx, client, fullDirPath, prefix, fn, startFrom, inclusive, limit, snapshotTsNs)
		return err
	})
	return actualSnapshotTsNs, err
}

func SeaweedList(ctx context.Context, client SeaweedFilerClient, parentDirectoryPath, prefix string, fn EachEntryFunction, startFrom string, inclusive bool, limit uint32) (err error) {
	return doSeaweedList(ctx, client, util.FullPath(parentDirectoryPath), prefix, fn, startFrom, inclusive, limit)
}

func doSeaweedList(ctx context.Context, client SeaweedFilerClient, fullDirPath util.FullPath, prefix string, fn EachEntryFunction, startFrom string, inclusive bool, limit uint32) (err error) {
	_, err = DoSeaweedListWithSnapshot(ctx, client, fullDirPath, prefix, fn, startFrom, inclusive, limit, 0)
	return err
}

func DoSeaweedListWithSnapshot(ctx context.Context, client SeaweedFilerClient, fullDirPath util.FullPath, prefix string, fn EachEntryFunction, startFrom string, inclusive bool, limit uint32, snapshotTsNs int64) (actualSnapshotTsNs int64, err error) {
	// Redundancy limit to make it correctly judge whether it is the last file.
	redLimit := limit

	if limit < math.MaxInt32 && limit != 0 {
		redLimit = limit + 1
	}
	if redLimit > math.MaxInt32 {
		redLimit = math.MaxInt32
	}
	request := &ListEntriesRequest{
		Directory:          string(fullDirPath),
		Prefix:             prefix,
		StartFromFileName:  startFrom,
		Limit:              redLimit,
		InclusiveStartFrom: inclusive,
		SnapshotTsNs:       snapshotTsNs,
	}

	// Preserve the caller-requested snapshot so pagination uses the same
	// boundary across pages. For first requests (snapshotTsNs==0) we do NOT
	// synthesize a client-side timestamp — if the server returns no entries,
	// we return 0 so callers like CompleteDirectoryBuild know no server
	// snapshot was received and can replay all buffered events without
	// clock-skew-sensitive filtering.
	actualSnapshotTsNs = snapshotTsNs

	glog.V(4).InfofCtx(ctx, "read directory: %v", request)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.ListEntries(ctx, request)
	if err != nil {
		return actualSnapshotTsNs, fmt.Errorf("list %s: %v", fullDirPath, err)
	}

	var prevEntry *Entry
	count := 0
	for {
		resp, recvErr := stream.Recv()
		if recvErr != nil {
			if recvErr == io.EOF {
				if prevEntry != nil {
					if err := fn(prevEntry, true); err != nil {
						return actualSnapshotTsNs, err
					}
				}
				break
			} else {
				return actualSnapshotTsNs, recvErr
			}
		}
		if resp.SnapshotTsNs != 0 {
			actualSnapshotTsNs = resp.SnapshotTsNs
		}
		if prevEntry != nil {
			if err := fn(prevEntry, false); err != nil {
				return actualSnapshotTsNs, err
			}
		}
		prevEntry = resp.Entry
		count++
		if count > int(limit) && limit != 0 {
			prevEntry = nil
		}
	}

	return actualSnapshotTsNs, nil
}

func Exists(ctx context.Context, filerClient FilerClient, parentDirectoryPath string, entryName string, isDirectory bool) (exists bool, err error) {

	err = filerClient.WithFilerClient(false, func(client SeaweedFilerClient) error {

		request := &LookupDirectoryEntryRequest{
			Directory: parentDirectoryPath,
			Name:      entryName,
		}

		glog.V(4).InfofCtx(ctx, "exists entry %v/%v: %v", parentDirectoryPath, entryName, request)
		resp, err := LookupEntry(ctx, client, request)
		if err != nil {
			if err == ErrNotFound {
				exists = false
				return nil
			}
			glog.V(0).InfofCtx(ctx, "exists entry %v: %v", request, err)
			return fmt.Errorf("exists entry %s/%s: %v", parentDirectoryPath, entryName, err)
		}

		exists = resp.Entry.IsDirectory == isDirectory

		return nil
	})

	return
}

func Mkdir(ctx context.Context, filerClient FilerClient, parentDirectoryPath string, dirName string, fn func(entry *Entry)) error {
	return filerClient.WithFilerClient(false, func(client SeaweedFilerClient) error {
		return DoMkdir(ctx, client, parentDirectoryPath, dirName, fn)
	})
}

func DoMkdir(ctx context.Context, client SeaweedFilerClient, parentDirectoryPath string, dirName string, fn func(entry *Entry)) error {
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

	glog.V(1).InfofCtx(ctx, "mkdir: %v", request)
	if err := CreateEntry(ctx, client, request); err != nil {
		glog.V(0).InfofCtx(ctx, "mkdir %v: %v", request, err)
		return fmt.Errorf("mkdir %s/%s: %v", parentDirectoryPath, dirName, err)
	}

	return nil
}

func MkFile(ctx context.Context, filerClient FilerClient, parentDirectoryPath string, fileName string, chunks []*FileChunk, fn func(entry *Entry)) error {
	return filerClient.WithFilerClient(false, func(client SeaweedFilerClient) error {

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

		if fn != nil {
			fn(entry)
		}

		request := &CreateEntryRequest{
			Directory: parentDirectoryPath,
			Entry:     entry,
		}

		glog.V(1).InfofCtx(ctx, "create file: %s/%s", parentDirectoryPath, fileName)
		if err := CreateEntry(ctx, client, request); err != nil {
			glog.V(0).InfofCtx(ctx, "create file %v:%v", request, err)
			return fmt.Errorf("create file %s/%s: %v", parentDirectoryPath, fileName, err)
		}

		return nil
	})
}

func Remove(ctx context.Context, filerClient FilerClient, parentDirectoryPath, name string, isDeleteData, isRecursive, ignoreRecursiveErr, isFromOtherCluster bool, signatures []int32) error {
	_, err := RemoveWithResponse(ctx, filerClient, parentDirectoryPath, name, isDeleteData, isRecursive, ignoreRecursiveErr, isFromOtherCluster, signatures)
	return err
}

func DoRemove(ctx context.Context, client SeaweedFilerClient, parentDirectoryPath string, name string, isDeleteData bool, isRecursive bool, ignoreRecursiveErr bool, isFromOtherCluster bool, signatures []int32) error {
	_, err := DoRemoveWithResponse(ctx, client, parentDirectoryPath, name, isDeleteData, isRecursive, ignoreRecursiveErr, isFromOtherCluster, signatures)
	return err
}

func RemoveWithResponse(ctx context.Context, filerClient FilerClient, parentDirectoryPath, name string, isDeleteData, isRecursive, ignoreRecursiveErr, isFromOtherCluster bool, signatures []int32) (*DeleteEntryResponse, error) {
	var resp *DeleteEntryResponse
	err := filerClient.WithFilerClient(false, func(client SeaweedFilerClient) error {
		var innerErr error
		resp, innerErr = DoRemoveWithResponse(ctx, client, parentDirectoryPath, name, isDeleteData, isRecursive, ignoreRecursiveErr, isFromOtherCluster, signatures)
		return innerErr
	})
	return resp, err
}

func DoRemoveWithResponse(ctx context.Context, client SeaweedFilerClient, parentDirectoryPath string, name string, isDeleteData bool, isRecursive bool, ignoreRecursiveErr bool, isFromOtherCluster bool, signatures []int32) (*DeleteEntryResponse, error) {
	deleteEntryRequest := &DeleteEntryRequest{
		Directory:            parentDirectoryPath,
		Name:                 name,
		IsDeleteData:         isDeleteData,
		IsRecursive:          isRecursive,
		IgnoreRecursiveError: ignoreRecursiveErr,
		IsFromOtherCluster:   isFromOtherCluster,
		Signatures:           signatures,
	}
	if resp, err := client.DeleteEntry(ctx, deleteEntryRequest); err != nil {
		if strings.Contains(err.Error(), ErrNotFound.Error()) {
			return nil, nil
		}
		return nil, err
	} else {
		if resp.Error != "" {
			if strings.Contains(resp.Error, ErrNotFound.Error()) {
				return nil, nil
			}
			return nil, errors.New(resp.Error)
		}
		return resp, nil
	}
}
