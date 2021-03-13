package wdclient

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/udptransfer"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/wdclient/net2"
	"io"
	"net"
	"time"
)

// VolumeUdpClient put/get/delete file chunks directly on volume servers without replication
type VolumeUdpClient struct {
	cp net2.ConnectionPool
}

type VolumeUdpConn struct {
	net.Conn
	bufWriter *bufio.Writer
	bufReader *bufio.Reader
}

func NewVolumeUdpClient() *VolumeUdpClient {
	MaxIdleTime := 10 * time.Second
	return &VolumeUdpClient{
		cp: net2.NewMultiConnectionPool(net2.ConnectionOptions{
			MaxActiveConnections: 16,
			MaxIdleConnections:   1,
			MaxIdleTime:          &MaxIdleTime,
			DialMaxConcurrency:   0,
			Dial: func(network string, address string) (net.Conn, error) {

				listener, err := udptransfer.NewEndpoint(&udptransfer.Params{
					LocalAddr:      "",
					Bandwidth:      100,
					FastRetransmit: true,
					FlatTraffic:    true,
					IsServ:         false,
				})
				if err != nil {
					return nil, err
				}

				conn, err := listener.Dial(address)
				if err != nil {
					return nil, err
				}
				return &VolumeUdpConn{
					conn,
					bufio.NewWriter(conn),
					bufio.NewReader(conn),
				}, err

			},
			NowFunc:      nil,
			ReadTimeout:  0,
			WriteTimeout: 0,
		}),
	}
}
func (c *VolumeUdpClient) PutFileChunk(volumeServerAddress string, fileId string, fileSize uint32, fileReader io.Reader) (err error) {

	udpAddress, parseErr := pb.ParseServerAddress(volumeServerAddress, 20001)
	if parseErr != nil {
		return parseErr
	}

	c.cp.Register("udp", udpAddress)
	udpConn, getErr := c.cp.Get("udp", udpAddress)
	if getErr != nil {
		return fmt.Errorf("get connection to %s: %v", udpAddress, getErr)
	}
	conn := udpConn.RawConn().(*VolumeUdpConn)
	defer func() {
		if err != nil {
			udpConn.DiscardConnection()
		} else {
			udpConn.ReleaseConnection()
		}
	}()

	buf := []byte("+" + fileId + "\n")
	_, err = conn.bufWriter.Write([]byte(buf))
	if err != nil {
		return
	}
	util.Uint32toBytes(buf[0:4], fileSize)
	_, err = conn.bufWriter.Write(buf[0:4])
	if err != nil {
		return
	}
	_, err = io.Copy(conn.bufWriter, fileReader)
	if err != nil {
		return
	}
	conn.bufWriter.Write([]byte("!\n"))
	conn.bufWriter.Flush()

	ret, _, err := conn.bufReader.ReadLine()
	if err != nil {
		glog.V(0).Infof("upload by udp: %v", err)
		return
	}
	if !bytes.HasPrefix(ret, []byte("+OK")) {
		glog.V(0).Infof("upload by udp: %v", string(ret))
	}

	return nil
}
