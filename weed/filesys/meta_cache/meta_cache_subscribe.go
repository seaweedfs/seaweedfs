package meta_cache

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func SubscribeMetaEvents(mc *MetaCache, selfSignature int32, client filer_pb.FilerClient, dir string, lastTsNs int64) error {

	processEventFn := func(resp *filer_pb.SubscribeMetadataResponse) error {
		message := resp.EventNotification

		for _, sig := range message.Signatures {
			if sig == selfSignature && selfSignature != 0 {
				return nil
			}
		}

		dir := resp.Directory
		var oldPath util.FullPath
		var newEntry *filer.Entry
		if message.OldEntry != nil {
			oldPath = util.NewFullPath(dir, message.OldEntry.Name)
			glog.V(4).Infof("deleting %v", oldPath)
		}

		if message.NewEntry != nil {
			if message.NewParentPath != "" {
				dir = message.NewParentPath
			}
			key := util.NewFullPath(dir, message.NewEntry.Name)
			glog.V(4).Infof("creating %v", key)
			newEntry = filer.FromPbEntry(dir, message.NewEntry)
		}
		err := mc.AtomicUpdateEntryFromFiler(context.Background(), oldPath, newEntry)
		if err == nil && message.OldEntry != nil && message.NewEntry != nil {
			key := util.NewFullPath(dir, message.NewEntry.Name)
			mc.invalidateFunc(key)
		}

		return err

	}

	for {
		err := client.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stream, err := client.SubscribeMetadata(ctx, &filer_pb.SubscribeMetadataRequest{
				ClientName: "mount",
				PathPrefix: dir,
				SinceNs:    lastTsNs,
				Signature:  selfSignature,
			})
			if err != nil {
				return fmt.Errorf("subscribe: %v", err)
			}

			for {
				resp, listenErr := stream.Recv()
				if listenErr == io.EOF {
					return nil
				}
				if listenErr != nil {
					return listenErr
				}

				if err := processEventFn(resp); err != nil {
					glog.Fatalf("process %v: %v", resp, err)
				}
				lastTsNs = resp.TsNs
			}
		})
		if err != nil {
			glog.Errorf("subscribing filer meta change: %v", err)
		}
		time.Sleep(time.Second)
	}
}
