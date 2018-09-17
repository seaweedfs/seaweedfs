package sink

import (
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"fmt"
	"strings"
	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"context"
	"sync"
)

type ReplicationSink interface {
	DeleteEntry(entry *filer_pb.Entry, deleteIncludeChunks bool) error
	CreateEntry(entry *filer_pb.Entry) error
	UpdateEntry(oldEntry, newEntry *filer_pb.Entry, deleteIncludeChunks bool) error
}

type FilerSink struct {
	grpcAddress string
	id          string
	dir         string
}

func (fs *FilerSink) Initialize(configuration util.Configuration) error {
	return fs.initialize(
		configuration.GetString("grpcAddress"),
		configuration.GetString("id"),
		configuration.GetString("directory"),
	)
}

func (fs *FilerSink) initialize(grpcAddress string, id string, dir string) (err error) {
	fs.grpcAddress = grpcAddress
	fs.id = id
	fs.dir = dir
	return nil
}

func (fs *FilerSink) DeleteEntry(entry *filer_pb.Entry, deleteIncludeChunks bool) error {
	return fs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		dir, name := filer2.FullPath(entry.Name).DirAndName()

		request := &filer_pb.DeleteEntryRequest{
			Directory:    dir,
			Name:         name,
			IsDirectory:  entry.IsDirectory,
			IsDeleteData: deleteIncludeChunks,
		}

		glog.V(1).Infof("delete entry: %v", request)
		_, err := client.DeleteEntry(context.Background(), request)
		if err != nil {
			glog.V(0).Infof("delete entry %s: %v", entry.Name, err)
			return fmt.Errorf("delete entry %s: %v", entry.Name, err)
		}

		return nil
	})
}

func (fs *FilerSink) CreateEntry(entry *filer_pb.Entry) error {

	replicatedChunks, err := replicateChunks(entry.Chunks)

	if err != nil {
		glog.V(0).Infof("replicate entry chunks %s: %v", entry.Name, err)
		return fmt.Errorf("replicate entry chunks %s: %v", entry.Name, err)
	}

	return fs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		dir, name := filer2.FullPath(entry.Name).DirAndName()

		request := &filer_pb.CreateEntryRequest{
			Directory: dir,
			Entry: &filer_pb.Entry{
				Name:        name,
				IsDirectory: entry.IsDirectory,
				Attributes:  entry.Attributes,
				Chunks:      replicatedChunks,
			},
		}

		glog.V(1).Infof("create: %v", request)
		if _, err := client.CreateEntry(context.Background(), request); err != nil {
			glog.V(0).Infof("create entry %s: %v", entry.Name, err)
			return fmt.Errorf("create entry %s: %v", entry.Name, err)
		}

		return nil
	})
}

func (fs *FilerSink) UpdateEntry(oldEntry, newEntry *filer_pb.Entry, deleteIncludeChunks bool) error {
	return nil
}

func (fs *FilerSink) withFilerClient(fn func(filer_pb.SeaweedFilerClient) error) error {

	grpcConnection, err := util.GrpcDial(fs.grpcAddress)
	if err != nil {
		return fmt.Errorf("fail to dial %s: %v", fs.grpcAddress, err)
	}
	defer grpcConnection.Close()

	client := filer_pb.NewSeaweedFilerClient(grpcConnection)

	return fn(client)
}

func volumeId(fileId string) string {
	lastCommaIndex := strings.LastIndex(fileId, ",")
	if lastCommaIndex > 0 {
		return fileId[:lastCommaIndex]
	}
	return fileId
}

func replicateChunks(sourceChunks []*filer_pb.FileChunk) (replicatedChunks []*filer_pb.FileChunk, err error) {
	if len(sourceChunks) == 0 {
		return
	}
	var wg sync.WaitGroup
	for _, s := range sourceChunks {
		wg.Add(1)
		go func(chunk *filer_pb.FileChunk) {
			defer wg.Done()
			replicatedChunk, e := replicateOneChunk(chunk)
			if e != nil {
				err = e
			}
			replicatedChunks = append(replicatedChunks, replicatedChunk)
		}(s)
	}
	wg.Wait()

	return
}

func replicateOneChunk(sourceChunk *filer_pb.FileChunk) (*filer_pb.FileChunk, error) {

	fileId, err := fetchAndWrite(sourceChunk)
	if err != nil {
		return nil, fmt.Errorf("copy %s: %v", sourceChunk.FileId, err)
	}

	return &filer_pb.FileChunk{
		FileId:       fileId,
		Offset:       sourceChunk.Offset,
		Size:         sourceChunk.Size,
		Mtime:        sourceChunk.Mtime,
		ETag:         sourceChunk.ETag,
		SourceFileId: sourceChunk.FileId,
	}, nil
}

func fetchAndWrite(sourceChunk *filer_pb.FileChunk) (fileId string, err error) {

	return
}
