package wdclient

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/wdclient/net2"
	"io"
	"net"
	"time"
)

// VolumeTcpClient put/get/delete file chunks directly on volume servers without replication
type VolumeTcpClient struct {
	cp net2.ConnectionPool
}

type VolumeTcpConn struct {
	net.Conn
	bufWriter *bufio.Writer
	bufReader *bufio.Reader
}

func NewVolumeTcpClient() *VolumeTcpClient {
	MaxIdleTime := 10 * time.Second
	return &VolumeTcpClient{
		cp: net2.NewMultiConnectionPool(net2.ConnectionOptions{
			MaxActiveConnections: 16,
			MaxIdleConnections:   1,
			MaxIdleTime:          &MaxIdleTime,
			DialMaxConcurrency:   0,
			Dial: func(network string, address string) (net.Conn, error) {
				conn, err := net.Dial(network, address)
				return &VolumeTcpConn{
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
func (c *VolumeTcpClient) PutFileChunk(volumeServerAddress string, fileId string, fileSize uint32, fileReader io.Reader) (err error) {

	tcpAddress, parseErr := pb.ParseServerAddress(volumeServerAddress, 20000)
	if parseErr != nil {
		return parseErr
	}

	c.cp.Register("tcp", tcpAddress)
	tcpConn, getErr := c.cp.Get("tcp", tcpAddress)
	if getErr != nil {
		return fmt.Errorf("get connection to %s: %v", tcpAddress, getErr)
	}
	conn := tcpConn.RawConn().(*VolumeTcpConn)
	defer func() {
		if err != nil {
			tcpConn.DiscardConnection()
		} else {
			tcpConn.ReleaseConnection()
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
		glog.V(0).Infof("upload by tcp: %v", err)
		return
	}
	if !bytes.HasPrefix(ret, []byte("+OK")) {
		glog.V(0).Infof("upload by tcp: %v", string(ret))
	}

	return nil
}
