package wdclient

import (
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/pin/tftp"
	"io"
)

// VolumeTcpClient put/get/delete file chunks directly on volume servers without replication
type VolumeUdpClient struct {
	udpClient *tftp.Client
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

	if c.udpClient == nil {
		c.udpClient, err = tftp.NewClient(udpAddress)
		if err != nil {
			return
		}
	}
	rf, err := c.udpClient.Send(fileId, "octet")
	if err != nil {
		return
	}
	_, err = rf.ReadFrom(fileReader)
	if err != nil {
		return
	}

	return
}
