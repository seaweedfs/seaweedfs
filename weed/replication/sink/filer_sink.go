package sink

import (
	"context"
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/replication/source"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type ReplicationSink interface {
	DeleteEntry(key string, entry *filer_pb.Entry, deleteIncludeChunks bool) error
	CreateEntry(key string, entry *filer_pb.Entry) error
	UpdateEntry(key string, oldEntry, newEntry *filer_pb.Entry, deleteIncludeChunks bool) error
	GetDirectory() string
	SetSourceFiler(s *source.FilerSource)
}

type FilerSink struct {
	grpcAddress string
	dir         string
	filerSource *source.FilerSource
	replication string
	collection  string
	ttlSec      int32
	dataCenter  string
}

func (fs *FilerSink) GetDirectory() string {
	return fs.dir
}

func (fs *FilerSink) Initialize(configuration util.Configuration) error {
	return fs.initialize(
		configuration.GetString("grpcAddress"),
		configuration.GetString("directory"),
	)
}

func (fs *FilerSink) SetSourceFiler(s *source.FilerSource) {
	fs.filerSource = s
}

func (fs *FilerSink) initialize(grpcAddress string, dir string) (err error) {
	fs.grpcAddress = grpcAddress
	fs.dir = dir
	return nil
}

func (fs *FilerSink) DeleteEntry(key string, entry *filer_pb.Entry, deleteIncludeChunks bool) error {
	return fs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		dir, name := filer2.FullPath(key).DirAndName()

		request := &filer_pb.DeleteEntryRequest{
			Directory:    dir,
			Name:         name,
			IsDirectory:  entry.IsDirectory,
			IsDeleteData: deleteIncludeChunks,
		}

		glog.V(1).Infof("delete entry: %v", request)
		_, err := client.DeleteEntry(context.Background(), request)
		if err != nil {
			glog.V(0).Infof("delete entry %s: %v", key, err)
			return fmt.Errorf("delete entry %s: %v", key, err)
		}

		return nil
	})
}

func (fs *FilerSink) CreateEntry(key string, entry *filer_pb.Entry) error {

	replicatedChunks, err := fs.replicateChunks(entry.Chunks)

	if err != nil {
		glog.V(0).Infof("replicate entry chunks %s: %v", key, err)
		return fmt.Errorf("replicate entry chunks %s: %v", key, err)
	}

	glog.V(0).Infof("replicated %s %+v ===> %+v", key, entry.Chunks, replicatedChunks)

	return fs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		dir, name := filer2.FullPath(key).DirAndName()

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
			glog.V(0).Infof("create entry %s: %v", key, err)
			return fmt.Errorf("create entry %s: %v", key, err)
		}

		return nil
	})
}

func (fs *FilerSink) UpdateEntry(key string, oldEntry, newEntry *filer_pb.Entry, deleteIncludeChunks bool) (err error) {

	ctx := context.Background()

	dir, name := filer2.FullPath(key).DirAndName()

	// find out what changed
	deletedChunks, newChunks := compareChunks(oldEntry, newEntry)

	// read existing entry
	var entry *filer_pb.Entry
	err = fs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		}

		glog.V(4).Infof("lookup directory entry: %v", request)
		resp, err := client.LookupDirectoryEntry(ctx, request)
		if err != nil {
			glog.V(0).Infof("lookup %s: %v", key, err)
			return err
		}

		entry = resp.Entry

		return nil
	})

	if err != nil {
		return err
	}

	// delete the chunks that are deleted from the source
	if deleteIncludeChunks {
		// remove the deleted chunks. Actual data deletion happens in filer UpdateEntry FindUnusedFileChunks
		entry.Chunks = minusChunks(entry.Chunks, deletedChunks)
	}

	// replicate the chunks that are new in the source
	replicatedChunks, err := fs.replicateChunks(newChunks)
	entry.Chunks = append(entry.Chunks, replicatedChunks...)

	// save updated meta data
	return fs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.UpdateEntryRequest{
			Directory: dir,
			Entry:     entry,
		}

		if _, err := client.UpdateEntry(ctx, request); err != nil {
			return fmt.Errorf("update entry %s: %v", key, err)
		}

		return nil
	})

}
func compareChunks(oldEntry, newEntry *filer_pb.Entry) (deletedChunks, newChunks []*filer_pb.FileChunk) {
	deletedChunks = minusChunks(oldEntry.Chunks, newEntry.Chunks)
	newChunks = minusChunks(newEntry.Chunks, oldEntry.Chunks)
	return
}

func minusChunks(as, bs []*filer_pb.FileChunk) (delta []*filer_pb.FileChunk) {
	for _, a := range as {
		found := false
		for _, b := range bs {
			if a.FileId == b.FileId {
				found = true
				break
			}
		}
		if !found {
			delta = append(delta, a)
		}
	}
	return
}
