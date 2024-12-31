package mount

import (
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (wfs *WFS) subscribeFilerConfEvents() (*meta_cache.MetadataFollower, error) {
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
		if errors.Is(err, filer_pb.ErrNotFound) {
			glog.V(0).Infof("fuse filer conf %s not found", confFullName)
		} else {
			return nil, err
		}
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
	return &meta_cache.MetadataFollower{
		PathPrefixToWatch: confFullName,
		ProcessEventFn:    processEventFn,
	}, nil
}

func (wfs *WFS) wormEnforcedForEntry(path util.FullPath, entry *filer_pb.Entry) (wormEnforced, wormEnabled bool) {
	if entry == nil || wfs.FilerConf == nil {
		return false, false
	}

	rule := wfs.FilerConf.MatchStorageRule(string(path))
	if !rule.Worm {
		return false, false
	}

	// worm is not enforced
	if entry.WormEnforcedAtTsNs == 0 {
		return false, true
	}

	// worm will never expire
	if rule.WormRetentionTimeSeconds == 0 {
		return true, true
	}

	enforcedAt := time.Unix(0, entry.WormEnforcedAtTsNs)

	// worm is expired
	if time.Now().Sub(enforcedAt).Seconds() >= float64(rule.WormRetentionTimeSeconds) {
		return false, true
	}

	return true, true
}
