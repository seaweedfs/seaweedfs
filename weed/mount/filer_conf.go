package mount

import (
	"fmt"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (wfs *WFS) subscribeFilerConfEvents() (func(), error) {
	now := time.Now()
	confDir := filer.DirectoryEtcSeaweedFS
	confName := filer.FilerConfName
	confFullName := filepath.Join(filer.DirectoryEtcSeaweedFS, filer.FilerConfName)

	// read current conf
	err := wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		content, err := filer.ReadInsideFiler(client, confDir, confName)
		if err != nil {
			return err
		}

		fc := filer.NewFilerConf()
		if len(content) > 0 {
			if err := fc.LoadFromBytes(content); err != nil {
				return fmt.Errorf("parse %s: %v", confFullName, err)
			}
		}

		wfs.FilerConf = fc

		return nil
	})
	if err != nil {
		return nil, err
	}

	processEventFn := func(resp *filer_pb.SubscribeMetadataResponse) error {
		message := resp.EventNotification
		if message.NewEntry == nil {
			return nil
		}

		dir := resp.Directory
		name := resp.EventNotification.NewEntry.Name

		if dir != confDir || name != confName {
			return nil
		}

		content := message.NewEntry.Content
		fc := filer.NewFilerConf()
		if len(content) > 0 {
			if err = fc.LoadFromBytes(content); err != nil {
				return fmt.Errorf("parse %s: %v", confFullName, err)
			}
		}

		wfs.FilerConf = fc

		return nil
	}

	metadataFollowOption := &pb.MetadataFollowOption{
		ClientName:             "fuse",
		ClientId:               wfs.signature,
		ClientEpoch:            1,
		SelfSignature:          0,
		PathPrefix:             confFullName,
		AdditionalPathPrefixes: nil,
		StartTsNs:              now.UnixNano(),
		StopTsNs:               0,
		EventErrorType:         pb.FatalOnError,
	}

	return func() {
		// sync new conf changes
		util.RetryUntil("followFilerConfChanges", func() error {
			metadataFollowOption.ClientEpoch++
			i := atomic.LoadInt32(&wfs.option.filerIndex)
			n := len(wfs.option.FilerAddresses)
			err = pb.FollowMetadata(wfs.option.FilerAddresses[i], wfs.option.GrpcDialOption, metadataFollowOption, processEventFn)
			if err == nil {
				atomic.StoreInt32(&wfs.option.filerIndex, i)
				return nil
			}

			i++
			if i >= int32(n) {
				i = 0
			}

			return err
		}, func(err error) bool {
			glog.V(0).Infof("fuse follow filer conf changes: %v", err)
			return true
		})
	}, nil
}

func (wfs *WFS) wormEnabledForEntry(path util.FullPath, entry *filer_pb.Entry) bool {
	if entry == nil || entry.Attributes == nil {
		return false
	}
	if wfs.FilerConf == nil {
		return false
	}

	rule := wfs.FilerConf.MatchStorageRule(string(path))
	if !rule.Worm {
		return false
	}

	return entry.Attributes.FileSize > 0 || entry.Attributes.Crtime != entry.Attributes.Mtime
}
