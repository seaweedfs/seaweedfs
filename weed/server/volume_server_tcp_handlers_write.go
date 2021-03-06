package weed_server

import (
	"bufio"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/util"
	"io"
	"net"
	"strings"
)

func (vs *VolumeServer) HandleTcpConnection(c net.Conn) {
	defer c.Close()

	glog.V(0).Infof("Serving writes from %s", c.RemoteAddr().String())

	bufReader := bufio.NewReaderSize(c, 1024*1024)
	bufWriter := bufio.NewWriterSize(c, 1024*1024)

	for {
		cmd, err := bufReader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				glog.Errorf("read command from %s: %v", c.RemoteAddr().String(), err)
			}
			return
		}
		cmd = cmd[:len(cmd)-1]
		switch cmd[0] {
		case '+':
			fileId := cmd[1:]
			err = vs.handleTcpPut(fileId, bufReader)
			if err == nil {
				bufWriter.Write([]byte("+OK\n"))
			} else {
				bufWriter.Write([]byte("-ERR " + string(err.Error()) + "\n"))
			}
		case '-':
			fileId := cmd[1:]
			err = vs.handleTcpDelete(fileId)
			if err == nil {
				bufWriter.Write([]byte("+OK\n"))
			} else {
				bufWriter.Write([]byte("-ERR " + string(err.Error()) + "\n"))
			}
		case '?':
			fileId := cmd[1:]
			err = vs.handleTcpGet(fileId, bufWriter)
		case '!':
			bufWriter.Flush()
		}

	}

}

func (vs *VolumeServer) handleTcpGet(fileId string, writer *bufio.Writer) (err error) {

	volumeId, n, err2 := vs.parseFileId(fileId)
	if err2 != nil {
		return err2
	}

	volume := vs.store.GetVolume(volumeId)
	if volume == nil {
		return fmt.Errorf("volume %d not found", volumeId)
	}

	err = volume.StreamRead(n, writer)
	if err != nil {
		return err
	}

	return nil
}

func (vs *VolumeServer) handleTcpPut(fileId string, bufReader *bufio.Reader) (err error) {

	volumeId, n, err2 := vs.parseFileId(fileId)
	if err2 != nil {
		return err2
	}

	volume := vs.store.GetVolume(volumeId)
	if volume == nil {
		return fmt.Errorf("volume %d not found", volumeId)
	}

	sizeBuf := make([]byte, 4)
	if _, err = bufReader.Read(sizeBuf); err != nil {
		return err
	}
	dataSize := util.BytesToUint32(sizeBuf)

	err = volume.StreamWrite(n, bufReader, dataSize)
	if err != nil {
		return err
	}

	return nil
}

func (vs *VolumeServer) handleTcpDelete(fileId string) (err error) {

	volumeId, n, err2 := vs.parseFileId(fileId)
	if err2 != nil {
		return err2
	}

	_, err = vs.store.DeleteVolumeNeedle(volumeId, n)
	if err != nil {
		return err
	}

	return nil
}

func (vs *VolumeServer) parseFileId(fileId string) (needle.VolumeId, *needle.Needle, error) {

	commaIndex := strings.LastIndex(fileId, ",")
	if commaIndex <= 0 {
		return 0, nil, fmt.Errorf("unknown fileId %s", fileId)
	}

	vid, fid := fileId[0:commaIndex], fileId[commaIndex+1:]

	volumeId, ve := needle.NewVolumeId(vid)
	if ve != nil {
		return 0, nil, fmt.Errorf("unknown volume id in fileId %s", fileId)
	}

	n := new(needle.Needle)
	n.ParsePath(fid)
	return volumeId, n, nil
}
