package filer

import (
	"bytes"
	"math"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

// onMetadataChangeEvent is triggered after filer processed change events from local or remote filers
func (f *Filer) onMetadataChangeEvent(event *filer_pb.SubscribeMetadataResponse) {
	if DirectoryEtcSeaweedFS != event.Directory {
		if DirectoryEtcSeaweedFS != event.EventNotification.NewParentPath {
			return
		}
	}

	entry := event.EventNotification.NewEntry
	if entry == nil {
		return
	}

	glog.V(0).Infof("procesing %v", event)
	if entry.Name == FilerConfName {
		f.reloadFilerConfiguration(entry)
	}

}

func (f *Filer) readEntry(chunks []*filer_pb.FileChunk) ([]byte, error) {
	var buf bytes.Buffer
	err := StreamContent(f.MasterClient, &buf, chunks, 0, math.MaxInt64)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (f *Filer) reloadFilerConfiguration(entry *filer_pb.Entry) {
	fc := NewFilerConf()
	err := fc.loadFromChunks(f, entry.Chunks)
	if err != nil {
		glog.Errorf("read filer conf chunks: %v", err)
		return
	}
	f.FilerConf = fc
}

func (f *Filer) LoadFilerConf() {
	fc := NewFilerConf()
	err := util.Retry("loadFilerConf", func() error {
		return fc.loadFromFiler(f)
	})
	if err != nil {
		glog.Errorf("read filer conf: %v", err)
		return
	}
	f.FilerConf = fc
}
