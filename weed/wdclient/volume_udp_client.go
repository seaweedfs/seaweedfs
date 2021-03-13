package wdclient

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"pack.ag/tftp"
	"io"
)

// VolumeTcpClient put/get/delete file chunks directly on volume servers without replication
type VolumeUdpClient struct {
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

	udpClient, _ := tftp.NewClient()

	fileUrl := "tftp://"+udpAddress+"/"+fileId

	err = udpClient.Put(fileUrl, fileReader, int64(fileSize))
	if err != nil {
		glog.Errorf("udp put %s: %v", fileUrl, err)
		return
	}

	return
}
