package filersink

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"math"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/security"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/sink"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type FilerSink struct {
	filerSource       *source.FilerSource
	grpcAddress       string
	dir               string
	replication       string
	collection        string
	ttlSec            int32
	diskType          string
	dataCenter        string
	grpcDialOption    grpc.DialOption
	address           string
	writeChunkByFiler bool
	isIncremental     bool
	executor          *util.LimitedConcurrentExecutor
	signature         int32
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

func (fs *FilerSink) IsIncremental() bool {
	return fs.isIncremental
}

func (fs *FilerSink) Initialize(configuration util.Configuration, prefix string) error {
	fs.isIncremental = configuration.GetBool(prefix + "is_incremental")
	fs.dataCenter = configuration.GetString(prefix + "dataCenter")
	fs.signature = util.RandomInt32()
	return fs.DoInitialize(
		"",
		configuration.GetString(prefix+"grpcAddress"),
		configuration.GetString(prefix+"directory"),
		configuration.GetString(prefix+"replication"),
		configuration.GetString(prefix+"collection"),
		configuration.GetInt(prefix+"ttlSec"),
		configuration.GetString(prefix+"disk"),
		security.LoadClientTLS(util.GetViper(), "grpc.client"),
		false)
}

func (fs *FilerSink) SetSourceFiler(s *source.FilerSource) {
	fs.filerSource = s
}

func (fs *FilerSink) DoInitialize(address, grpcAddress string, dir string,
	replication string, collection string, ttlSec int, diskType string, grpcDialOption grpc.DialOption, writeChunkByFiler bool) (err error) {
	fs.address = address
	if fs.address == "" {
		fs.address = pb.GrpcAddressToServerAddress(grpcAddress)
	}
	fs.grpcAddress = grpcAddress
	fs.dir = dir
	fs.replication = replication
	fs.collection = collection
	fs.ttlSec = int32(ttlSec)
	fs.diskType = diskType
	fs.grpcDialOption = grpcDialOption
	fs.writeChunkByFiler = writeChunkByFiler
	fs.executor = util.NewLimitedConcurrentExecutor(32)
	return nil
}

func (fs *FilerSink) DeleteEntry(key string, isDirectory, deleteIncludeChunks bool, signatures []int32) error {

	dir, name := util.FullPath(key).DirAndName()

	glog.V(4).Infof("delete entry: %v", key)
	err := filer_pb.Remove(fs, dir, name, deleteIncludeChunks, true, true, true, signatures)
	if err != nil {
		glog.V(0).Infof("delete entry %s: %v", key, err)
		return fmt.Errorf("delete entry %s: %v", key, err)
	}
	return nil
}

func (fs *FilerSink) CreateEntry(key string, entry *filer_pb.Entry, signatures []int32) error {

	return fs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		dir, name := util.FullPath(key).DirAndName()

		// look up existing entry
		lookupRequest := &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		}
		// glog.V(1).Infof("lookup: %v", lookupRequest)
		if resp, err := filer_pb.LookupEntry(client, lookupRequest); err == nil {
			if filer.ETag(resp.Entry) == filer.ETag(entry) {
				glog.V(3).Infof("already replicated %s", key)
				return nil
			}
			if resp.Entry.Attributes != nil && resp.Entry.Attributes.Mtime >= entry.Attributes.Mtime {
				glog.V(3).Infof("skip overwriting %s", key)
				return nil
			}
		}

		replicatedChunks, err := fs.replicateChunks(entry.GetChunks(), key)

		if err != nil {
			// only warning here since the source chunk may have been deleted already
			glog.Warningf("replicate entry chunks %s: %v", key, err)
			return nil
		}

		// glog.V(4).Infof("replicated %s %+v ===> %+v", key, entry.GetChunks(), replicatedChunks)

		request := &filer_pb.CreateEntryRequest{
			Directory: dir,
			Entry: &filer_pb.Entry{
				Name:        name,
				IsDirectory: entry.IsDirectory,
				Attributes:  entry.Attributes,
				Extended:    entry.Extended,
				Chunks:      replicatedChunks,
				Content:     entry.Content,
				RemoteEntry: entry.RemoteEntry,
			},
			IsFromOtherCluster: true,
			Signatures:         signatures,
		}

		glog.V(3).Infof("create: %v", request)
		if err := filer_pb.CreateEntry(client, request); err != nil {
			glog.V(0).Infof("create entry %s: %v", key, err)
			return fmt.Errorf("create entry %s: %v", key, err)
		}

		return nil
	})
}

func (fs *FilerSink) UpdateEntry(key string, oldEntry *filer_pb.Entry, newParentPath string, newEntry *filer_pb.Entry, deleteIncludeChunks bool, signatures []int32) (foundExistingEntry bool, err error) {

	dir, name := util.FullPath(key).DirAndName()

	// read existing entry
	var existingEntry *filer_pb.Entry
	err = fs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		}

		glog.V(4).Infof("lookup entry: %v", request)
		resp, err := filer_pb.LookupEntry(client, request)
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

	glog.V(4).Infof("oldEntry %+v, newEntry %+v, existingEntry: %+v", oldEntry, newEntry, existingEntry)

	if existingEntry.Attributes.Mtime > newEntry.Attributes.Mtime {
		// skip if already changed
		// this usually happens when the messages are not ordered
		glog.V(2).Infof("late updates %s", key)
	} else {
		// find out what changed
		deletedChunks, newChunks, err := compareChunks(filer.LookupFn(fs), oldEntry, newEntry)
		if err != nil {
			return true, fmt.Errorf("replicate %s compare chunks error: %v", key, err)
		}

		// delete the chunks that are deleted from the source
		if deleteIncludeChunks {
			// remove the deleted chunks. Actual data deletion happens in filer UpdateEntry FindUnusedFileChunks
			existingEntry.Chunks = filer.DoMinusChunksBySourceFileId(existingEntry.GetChunks(), deletedChunks)
		}

		// replicate the chunks that are new in the source
		replicatedChunks, err := fs.replicateChunks(newChunks, key)
		if err != nil {
			glog.Warningf("replicate entry chunks %s: %v", key, err)
			return true, nil
		}
		existingEntry.Chunks = append(existingEntry.GetChunks(), replicatedChunks...)
		existingEntry.Attributes = newEntry.Attributes
		existingEntry.Extended = newEntry.Extended
		existingEntry.HardLinkId = newEntry.HardLinkId
		existingEntry.HardLinkCounter = newEntry.HardLinkCounter
		existingEntry.Content = newEntry.Content
		existingEntry.RemoteEntry = newEntry.RemoteEntry
	}

	// save updated meta data
	return true, fs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.UpdateEntryRequest{
			Directory:          newParentPath,
			Entry:              existingEntry,
			IsFromOtherCluster: true,
			Signatures:         signatures,
		}

		if _, err := client.UpdateEntry(context.Background(), request); err != nil {
			return fmt.Errorf("update existingEntry %s: %v", key, err)
		}

		return nil
	})

}
func compareChunks(lookupFileIdFn wdclient.LookupFileIdFunctionType, oldEntry, newEntry *filer_pb.Entry) (deletedChunks, newChunks []*filer_pb.FileChunk, err error) {
	aData, aMeta, aErr := filer.ResolveChunkManifest(lookupFileIdFn, oldEntry.GetChunks(), 0, math.MaxInt64)
	if aErr != nil {
		return nil, nil, aErr
	}
	bData, bMeta, bErr := filer.ResolveChunkManifest(lookupFileIdFn, newEntry.GetChunks(), 0, math.MaxInt64)
	if bErr != nil {
		return nil, nil, bErr
	}

	deletedChunks = append(deletedChunks, filer.DoMinusChunks(aData, bData)...)
	deletedChunks = append(deletedChunks, filer.DoMinusChunks(aMeta, bMeta)...)

	newChunks = append(newChunks, filer.DoMinusChunks(bData, aData)...)
	newChunks = append(newChunks, filer.DoMinusChunks(bMeta, aMeta)...)

	return
}
