package replication

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"google.golang.org/grpc"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/replication/sink"
	"github.com/chrislusf/seaweedfs/weed/replication/source"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type Replicator struct {
	sink   sink.ReplicationSink
	source *source.FilerSource
}

func NewReplicator(sourceConfig util.Configuration, configPrefix string, dataSink sink.ReplicationSink) *Replicator {

	source := &source.FilerSource{}
	source.Initialize(sourceConfig, configPrefix)

	dataSink.SetSourceFiler(source)

	return &Replicator{
		sink:   dataSink,
		source: source,
	}
}

func (r *Replicator) Replicate(ctx context.Context, key string, message *filer_pb.EventNotification) error {
	if message.IsFromOtherCluster && r.sink.GetName() == "filer" {
		return nil
	}
	if !strings.HasPrefix(key, r.source.Dir) {
		glog.V(4).Infof("skipping %v outside of %v", key, r.source.Dir)
		return nil
	}
	var dateKey string
	if r.sink.IsIncremental() {
		var mTime int64
		if message.NewEntry != nil {
			mTime = message.NewEntry.Attributes.Mtime
		} else if message.OldEntry != nil {
			mTime = message.OldEntry.Attributes.Mtime
		}
		dateKey = time.Unix(mTime, 0).Format("2006-01-02")
	}
	newKey := util.Join(r.sink.GetSinkToDirectory(), dateKey, key[len(r.source.Dir):])
	glog.V(3).Infof("replicate %s => %s", key, newKey)
	key = newKey
	if message.OldEntry != nil && message.NewEntry == nil {
		glog.V(4).Infof("deleting %v", key)
		return r.sink.DeleteEntry(key, message.OldEntry.IsDirectory, message.DeleteChunks, message.Signatures)
	}
	if message.OldEntry == nil && message.NewEntry != nil {
		glog.V(4).Infof("creating %v", key)
		return r.sink.CreateEntry(key, message.NewEntry, message.Signatures)
	}
	if message.OldEntry == nil && message.NewEntry == nil {
		glog.V(0).Infof("weird message %+v", message)
		return nil
	}

	foundExisting, err := r.sink.UpdateEntry(key, message.OldEntry, message.NewParentPath, message.NewEntry, message.DeleteChunks, message.Signatures)
	if foundExisting {
		glog.V(4).Infof("updated %v", key)
		return err
	}

	err = r.sink.DeleteEntry(key, message.OldEntry.IsDirectory, false, message.Signatures)
	if err != nil {
		return fmt.Errorf("delete old entry %v: %v", key, err)
	}

	glog.V(4).Infof("creating missing %v", key)
	return r.sink.CreateEntry(key, message.NewEntry, message.Signatures)
}

func ReadFilerSignature(grpcDialOption grpc.DialOption, filer string) (filerSignature int32, readErr error) {
	if readErr = pb.WithFilerClient(filer, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
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
