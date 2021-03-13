package wdclient

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"io"
	"pack.ag/tftp"
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

	udpClient, _ := tftp.NewClient(
		tftp.ClientMode(tftp.ModeOctet),
		tftp.ClientBlocksize(9000),
		tftp.ClientWindowsize(16),
		tftp.ClientTimeout(1),
		tftp.ClientTransferSize(true),
		tftp.ClientRetransmit(3),
	)

	fileUrl := "tftp://" + udpAddress + "/" + fileId

	// println("put", fileUrl, "...")
	err = udpClient.Put(fileUrl, fileReader, int64(fileSize))
	if err != nil {
		glog.Errorf("udp put %s: %v", fileUrl, err)
		return
	}
	// println("sent", fileUrl)

	return
}
