package filersink

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/spf13/viper"
	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/replication/sink"
	"github.com/chrislusf/seaweedfs/weed/replication/source"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type FilerSink struct {
	filerSource    *source.FilerSource
	grpcAddress    string
	dir            string
	replication    string
	collection     string
	ttlSec         int32
	dataCenter     string
	grpcDialOption grpc.DialOption
}

func init() {
	sink.Sinks = append(sink.Sinks, &FilerSink{})
}

func (fs *FilerSink) GetName() string {
	return "filer"
}

func (fs *FilerSink) GetSinkToDirectory() string {
	return fs.dir
}

func (fs *FilerSink) Initialize(configuration util.Configuration) error {
	return fs.initialize(
		configuration.GetString("grpcAddress"),
		configuration.GetString("directory"),
		configuration.GetString("replication"),
		configuration.GetString("collection"),
		configuration.GetInt("ttlSec"),
	)
}

func (fs *FilerSink) SetSourceFiler(s *source.FilerSource) {
	fs.filerSource = s
}

func (fs *FilerSink) initialize(grpcAddress string, dir string,
	replication string, collection string, ttlSec int) (err error) {
	fs.grpcAddress = grpcAddress
	fs.dir = dir
	fs.replication = replication
	fs.collection = collection
	fs.ttlSec = int32(ttlSec)
	fs.grpcDialOption = security.LoadClientTLS(viper.Sub("grpc"), "client")
	return nil
}

func (fs *FilerSink) DeleteEntry(key string, isDirectory, deleteIncludeChunks bool) error {
	return fs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		dir, name := filer2.FullPath(key).DirAndName()

		request := &filer_pb.DeleteEntryRequest{
			Directory:    dir,
			Name:         name,
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

	return fs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		dir, name := filer2.FullPath(key).DirAndName()
		ctx := context.Background()

		// look up existing entry
		lookupRequest := &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		}
		glog.V(1).Infof("lookup: %v", lookupRequest)
		if resp, err := client.LookupDirectoryEntry(ctx, lookupRequest); err == nil {
			if filer2.ETag(resp.Entry.Chunks) == filer2.ETag(entry.Chunks) {
				glog.V(0).Infof("already replicated %s", key)
				return nil
			}
		}

		replicatedChunks, err := fs.replicateChunks(entry.Chunks)

		if err != nil {
			glog.V(0).Infof("replicate entry chunks %s: %v", key, err)
			return fmt.Errorf("replicate entry chunks %s: %v", key, err)
		}

		glog.V(0).Infof("replicated %s %+v ===> %+v", key, entry.Chunks, replicatedChunks)

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
		if _, err := client.CreateEntry(ctx, request); err != nil {
			glog.V(0).Infof("create entry %s: %v", key, err)
			return fmt.Errorf("create entry %s: %v", key, err)
		}

		return nil
	})
}

func (fs *FilerSink) UpdateEntry(key string, oldEntry, newEntry *filer_pb.Entry, deleteIncludeChunks bool) (foundExistingEntry bool, err error) {

	ctx := context.Background()

	dir, name := filer2.FullPath(key).DirAndName()

	// read existing entry
	var existingEntry *filer_pb.Entry
	err = fs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		}

		glog.V(4).Infof("lookup entry: %v", request)
		resp, err := client.LookupDirectoryEntry(ctx, request)
		if err != nil {
			glog.V(0).Infof("lookup %s: %v", key, err)
			return err
		}

		existingEntry = resp.Entry

		return nil
	})

	if err != nil {
		return false, fmt.Errorf("lookup %s: %v", key, err)
	}

	glog.V(0).Infof("oldEntry %+v, newEntry %+v, existingEntry: %+v", oldEntry, newEntry, existingEntry)

	if existingEntry.Attributes.Mtime > newEntry.Attributes.Mtime {
		// skip if already changed
		// this usually happens when the messages are not ordered
		glog.V(0).Infof("late updates %s", key)
	} else if filer2.ETag(newEntry.Chunks) == filer2.ETag(existingEntry.Chunks) {
		// skip if no change
		// this usually happens when retrying the replication
		glog.V(0).Infof("already replicated %s", key)
	} else {
		// find out what changed
		deletedChunks, newChunks := compareChunks(oldEntry, newEntry)

		// delete the chunks that are deleted from the source
		if deleteIncludeChunks {
			// remove the deleted chunks. Actual data deletion happens in filer UpdateEntry FindUnusedFileChunks
			existingEntry.Chunks = minusChunks(existingEntry.Chunks, deletedChunks)
		}

		// replicate the chunks that are new in the source
		replicatedChunks, err := fs.replicateChunks(newChunks)
		if err != nil {
			return true, fmt.Errorf("replicte %s chunks error: %v", key, err)
		}
		existingEntry.Chunks = append(existingEntry.Chunks, replicatedChunks...)
	}

	// save updated meta data
	return true, fs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.UpdateEntryRequest{
			Directory: dir,
			Entry:     existingEntry,
		}

		if _, err := client.UpdateEntry(ctx, request); err != nil {
			return fmt.Errorf("update existingEntry %s: %v", key, err)
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
