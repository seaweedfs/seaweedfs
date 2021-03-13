package weed_server

import (
	"bytes"
	"fmt"
	"io"
)

func (vs *VolumeServer) UdpReadHandler(filename string, rf io.ReaderFrom) error {

	volumeId, n, err := vs.parseFileId(filename)
	if err != nil {
		return err
	}

	hasVolume := vs.store.HasVolume(volumeId)
	_, hasEcVolume := vs.store.FindEcVolume(volumeId)

	if hasVolume {
		if _, err = vs.store.ReadVolumeNeedle(volumeId, n, nil); err != nil {
			return err
		}
	}
	if hasEcVolume {
		if _, err = vs.store.ReadEcShardNeedle(volumeId, n); err != nil {
			return err
		}
	}

	if _, err = rf.ReadFrom(bytes.NewBuffer(n.Data)); err != nil {
		return err
	}

	return nil
}

func (vs *VolumeServer) UdpWriteHandler(filename string, wt io.WriterTo) error {

	if filename[0] == '-' {
		return vs.handleTcpDelete(filename[1:])
	}

	volumeId, n, err := vs.parseFileId(filename)
	if err != nil {
		return err
	}

	volume := vs.store.GetVolume(volumeId)
	if volume == nil {
		return fmt.Errorf("volume %d not found", volumeId)
	}

	var buf bytes.Buffer
	written, err := wt.WriteTo(&buf)
	if err != nil {
		return err
	}

	err = volume.StreamWrite(n, &buf, uint32(written))
	if err != nil {
		return err
	}

	return nil
}
