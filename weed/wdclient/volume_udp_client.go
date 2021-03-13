package wdclient

import (
	"bufio"
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
	bufWriter.Write([]byte("!\n"))
	bufWriter.Flush()

	return nil
}
