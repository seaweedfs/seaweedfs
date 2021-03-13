package weed_server

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"pack.ag/tftp"
)

func (vs *VolumeServer) ServeTFTP(r tftp.ReadRequest) {

	filename := r.Name()

	volumeId, n, err := vs.parseFileId(filename)
	if err != nil {
		glog.Errorf("parse file id %s: %v", filename, err)
		return
	}

	hasVolume := vs.store.HasVolume(volumeId)
	_, hasEcVolume := vs.store.FindEcVolume(volumeId)

	if hasVolume {
		if _, err = vs.store.ReadVolumeNeedle(volumeId, n, nil); err != nil {
			glog.Errorf("ReadVolumeNeedle %s: %v", filename, err)
			return
		}
	}
	if hasEcVolume {
		if _, err = vs.store.ReadEcShardNeedle(volumeId, n); err != nil {
			glog.Errorf("ReadEcShardNeedle %s: %v", filename, err)
			return
		}
	}

	if _, err = r.Write(n.Data); err != nil {
		glog.Errorf("UDP Write data %s: %v", filename, err)
		return
	}

}

func (vs *VolumeServer) ReceiveTFTP(w tftp.WriteRequest) {

	filename := w.Name()

	// Get the file size
	size, err := w.Size()

	// Note: The size value is sent by the client, the client could send more data than
	// it indicated in the size option. To be safe we'd want to allocate a buffer
	// with the size we're expecting and use w.Read(buf) rather than ioutil.ReadAll.

	if filename[0] == '-' {
		err = vs.handleTcpDelete(filename[1:])
		if err != nil {
			glog.Errorf("handleTcpDelete %s: %v", filename, err)
			return
		}
	}

	volumeId, n, err := vs.parseFileId(filename)
	if err != nil {
		glog.Errorf("parse file id %s: %v", filename, err)
		return
	}

	volume := vs.store.GetVolume(volumeId)
	if volume == nil {
		glog.Errorf("volume %d not found", volumeId)
		return
	}

	err = volume.StreamWrite(n, w, uint32(size))
	if err != nil {
		glog.Errorf("StreamWrite %s: %v", filename, err)
		return
	}

}
