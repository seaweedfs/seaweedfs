package wdclient

import (
	"bufio"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/udptransfer"
	"github.com/chrislusf/seaweedfs/weed/util"
	"io"
	"net"
)

// VolumeUdpClient put/get/delete file chunks directly on volume servers without replication
type VolumeUdpClient struct {
}

type VolumeUdpConn struct {
	net.Conn
	bufWriter *bufio.Writer
	bufReader *bufio.Reader
}

func NewVolumeUdpClient() *VolumeUdpClient {
	return &VolumeUdpClient{
	}
}
func (c *VolumeUdpClient) PutFileChunk(volumeServerAddress string, fileId string, fileSize uint32, fileReader io.Reader) (err error) {

	udpAddress, parseErr := pb.ParseServerAddress(volumeServerAddress, 20001)
	if parseErr != nil {
		return parseErr
	}

	listener, err := udptransfer.NewEndpoint(&udptransfer.Params{
		LocalAddr:      "",
		Bandwidth:      100,
		FastRetransmit: true,
		FlatTraffic:    true,
		IsServ:         false,
	})
	if err != nil {
		return err
	}
	defer listener.Close()

	conn, err := listener.Dial(udpAddress)
	if err != nil {
		return err
	}
	defer conn.Close()

	bufWriter := bufio.NewWriter(conn)

	buf := []byte("+" + fileId + "\n")
	_, err = bufWriter.Write([]byte(buf))
	if err != nil {
		return
	}
	util.Uint32toBytes(buf[0:4], fileSize)
	_, err = bufWriter.Write(buf[0:4])
	if err != nil {
		return
	}
	_, err = io.Copy(bufWriter, fileReader)
	if err != nil {
		return
	}
	bufWriter.Flush()

	return nil
}
