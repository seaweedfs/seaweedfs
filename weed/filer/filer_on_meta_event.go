package filer

import (
	"bytes"
	"math"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

const (
	DirectoryEtc = "/etc"
)

// onMetadataChangeEvent is triggered after filer processed change events from local or remote filers
func (f *Filer) onMetadataChangeEvent(event *filer_pb.SubscribeMetadataResponse) {
	if DirectoryEtc != event.Directory {
		if DirectoryEtc != event.EventNotification.NewParentPath {
			return
		}
	}

	entry := event.EventNotification.NewEntry
	if entry == nil {
		return
	}

	glog.V(0).Infof("procesing %v", event)
	if entry.Name == "filer.conf" {
		f.reloadFilerConfiguration(entry)
	}

}

func (f *Filer) readEntry(entry *filer_pb.Entry) ([]byte, error){
	var buf bytes.Buffer
	err := StreamContent(f.MasterClient, &buf, entry.Chunks, 0, math.MaxInt64)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (f *Filer) reloadFilerConfiguration(entry *filer_pb.Entry) {
	data, err := f.readEntry(entry)
	if err != nil {
		glog.Warningf("read entry %s: %v", entry.Name, err)
		return

	}

	println(string(data))

}
