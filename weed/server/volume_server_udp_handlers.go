package weed_server

import (
	"bufio"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"io"
	"net"
)

func (vs *VolumeServer) HandleUdpConnection(c net.Conn) {
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
			if err != nil {
				glog.Errorf("put %s: %v", fileId, err)
			}
		case '-':
			fileId := cmd[1:]
			err = vs.handleTcpDelete(fileId)
			if err != nil {
				glog.Errorf("del %s: %v", fileId, err)
			}
		case '?':
			fileId := cmd[1:]
			err = vs.handleTcpGet(fileId, bufWriter)
		case '!':
		}

	}

}
