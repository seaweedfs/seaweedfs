package wdclient

import (
	"bufio"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/wdclient/penet"
	"io"
	"net"
	"time"
)

// VolumeUdpClient put/get/delete file chunks directly on volume servers without replication
type VolumeUdpClient struct {
	Conn      net.Conn
	bufWriter *bufio.Writer
	bufReader *bufio.Reader
}

type VolumeUdpConn struct {
	Conn      net.Conn
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

	if c.Conn == nil {
		c.Conn, err = penet.DialTimeout("", udpAddress, 500*time.Millisecond)
		if err != nil {
			return err
		}
		c.bufWriter = bufio.NewWriter(c.Conn)
	}

	buf := []byte("+" + fileId + "\n")
	_, err = c.bufWriter.Write([]byte(buf))
	if err != nil {
		return
	}
	util.Uint32toBytes(buf[0:4], fileSize)
	_, err = c.bufWriter.Write(buf[0:4])
	if err != nil {
		return
	}
	_, err = io.Copy(c.bufWriter, fileReader)
	if err != nil {
		return
	}
	c.bufWriter.Flush()

	return nil
}
