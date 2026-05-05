package replication

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/repl_util"
	"github.com/seaweedfs/seaweedfs/weed/replication/sink"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

type Replicator struct {
	sink        sink.ReplicationSink
	source      *source.FilerSource
	excludeDirs []string
}

func NewReplicator(sourceConfig util.Configuration, configPrefix string, dataSink sink.ReplicationSink) *Replicator {

	source := &source.FilerSource{}
	source.Initialize(sourceConfig, configPrefix)

	if err := repl_util.InitializeSSEForReplication(source); err != nil {
		glog.Warningf("SSE initialization failed: %v (encrypted objects may fail to replicate)", err)
	}

	dataSink.SetSourceFiler(source)

	return &Replicator{
		sink:        dataSink,
		source:      source,
		excludeDirs: sourceConfig.GetStringSlice(configPrefix + "excludeDirectories"),
	}
}

func (r *Replicator) Replicate(ctx context.Context, key string, message *filer_pb.EventNotification) error {
	if message.IsFromOtherCluster && r.sink.GetName() == "filer" {
		return nil
	}

	oldEntry := message.OldEntry
	newEntry := message.NewEntry
	newParentPath := message.NewParentPath

	oldInSource := util.IsEqualOrUnder(key, r.source.Dir) && !r.isExcluded(key)

	// For rename events (both old and new entry present), check both paths
	// against the source directory. Convert cross-boundary renames to
	// create or delete so the sink stays consistent.
	if oldEntry != nil && newEntry != nil {
		newFullPath, targetParent := metadataEventTarget(key, newEntry, newParentPath)
		newInSource := util.IsEqualOrUnder(newFullPath, r.source.Dir) && !r.isExcluded(newFullPath)

		if !oldInSource && !newInSource {
			return nil
		}
		if !oldInSource {
			// Rename into watched directory: treat as create
			oldEntry = nil
			key = newFullPath
			newParentPath = targetParent
		} else if !newInSource {
			// Rename out of watched directory: treat as delete
			newEntry = nil
			newParentPath = ""
		}
	} else if !oldInSource {
		glog.V(4).Infof("skipping %v outside of %v", key, r.source.Dir)
		return nil
	}

	var dateKey string
	if r.sink.IsIncremental() {
		var mTime int64
		if newEntry != nil {
			mTime = newEntry.Attributes.Mtime
		} else if oldEntry != nil {
			mTime = oldEntry.Attributes.Mtime
		}
		dateKey = time.Unix(mTime, 0).Format("2006-01-02")
	}
	oldSinkKey := r.sourceToSinkKey(key, dateKey)
	glog.V(3).Infof("replicate %s => %s", key, oldSinkKey)

	newSinkKey := oldSinkKey
	newSinkParentPath := newParentPath
	if oldEntry != nil && newEntry != nil {
		targetSourceKey, targetSourceParent := metadataEventTarget(key, newEntry, newParentPath)
		newSinkKey = r.sourceToSinkKey(targetSourceKey, dateKey)
		newSinkParentPath = r.sourceToSinkPath(targetSourceParent, dateKey)
	} else if newParentPath != "" && util.IsEqualOrUnder(newParentPath, r.source.Dir) {
		newSinkParentPath = r.sourceToSinkPath(newParentPath, dateKey)
	}

	if oldEntry != nil && newEntry == nil {
		glog.V(4).Infof("deleting %v", oldSinkKey)
		return r.sink.DeleteEntry(oldSinkKey, oldEntry.IsDirectory, message.DeleteChunks, message.Signatures)
	}
	if oldEntry == nil && newEntry != nil {
		glog.V(4).Infof("creating %v", oldSinkKey)
		return r.sink.CreateEntry(oldSinkKey, newEntry, message.Signatures)
	}
	if oldEntry == nil && newEntry == nil {
		glog.V(0).Infof("weird message %+v", message)
		return nil
	}

	if oldSinkKey != newSinkKey && r.sink.GetName() != "filer" {
		if err := r.sink.DeleteEntry(oldSinkKey, oldEntry.IsDirectory, false, message.Signatures); err != nil {
			return fmt.Errorf("delete old entry %v: %w", oldSinkKey, err)
		}
		glog.V(4).Infof("creating renamed %v", newSinkKey)
		return r.sink.CreateEntry(newSinkKey, newEntry, message.Signatures)
	}

	foundExisting, err := r.sink.UpdateEntry(oldSinkKey, oldEntry, newSinkParentPath, newEntry, message.DeleteChunks, message.Signatures)
	if foundExisting {
		glog.V(4).Infof("updated %v", oldSinkKey)
		return err
	}

	err = r.sink.DeleteEntry(oldSinkKey, oldEntry.IsDirectory, false, message.Signatures)
	if err != nil {
		return fmt.Errorf("delete old entry %v: %w", oldSinkKey, err)
	}

	glog.V(4).Infof("creating missing %v", newSinkKey)
	return r.sink.CreateEntry(newSinkKey, newEntry, message.Signatures)
}

func (r *Replicator) isExcluded(path string) bool {
	for _, excludeDir := range r.excludeDirs {
		if util.IsEqualOrUnder(path, excludeDir) {
			return true
		}
	}
	return false
}

func (r *Replicator) sourceToSinkKey(sourceKey, dateKey string) string {
	return util.Join(r.sink.GetSinkToDirectory(), dateKey, sourceKey[len(r.source.Dir):])
}

func (r *Replicator) sourceToSinkPath(sourcePath, dateKey string) string {
	return util.Join(r.sink.GetSinkToDirectory(), dateKey, sourcePath[len(r.source.Dir):])
}

func metadataEventTarget(key string, newEntry *filer_pb.Entry, newParentPath string) (targetKey, targetParent string) {
	if newEntry == nil {
		return "", ""
	}

	targetParent = newParentPath
	if targetParent == "" {
		targetParent, _ = util.FullPath(key).DirAndName()
	}

	return util.Join(targetParent, newEntry.Name), targetParent
}

func ReadFilerSignature(grpcDialOption grpc.DialOption, filer pb.ServerAddress) (filerSignature int32, readErr error) {
	if readErr = pb.WithFilerClient(false, 0, filer, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		if resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{}); err != nil {
			return fmt.Errorf("GetFilerConfiguration %s: %v", filer, err)
		} else {
			filerSignature = resp.Signature
		}
		return nil
	}); readErr != nil {
		return 0, readErr
	}
	return filerSignature, nil
}
