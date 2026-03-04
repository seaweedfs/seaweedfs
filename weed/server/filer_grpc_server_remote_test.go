package weed_server

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/stretchr/testify/assert"
)

func TestCacheConcurrencyDefault(t *testing.T) {
	conf := &remote_pb.RemoteConf{}
	cacheConcurrency := 8
	if conf.CacheConcurrency > 0 {
		cacheConcurrency = int(conf.CacheConcurrency)
	}
	assert.Equal(t, 8, cacheConcurrency)
}

func TestCacheConcurrencyCustom(t *testing.T) {
	conf := &remote_pb.RemoteConf{CacheConcurrency: 16}
	cacheConcurrency := 8
	if conf.CacheConcurrency > 0 {
		cacheConcurrency = int(conf.CacheConcurrency)
	}
	assert.Equal(t, 16, cacheConcurrency)
}

func TestDownloadConcurrencyDefault(t *testing.T) {
	conf := &remote_pb.RemoteConf{}
	downloadConcurrency := 1
	if conf.DownloadConcurrency > 0 {
		downloadConcurrency = int(conf.DownloadConcurrency)
	}
	assert.Equal(t, 1, downloadConcurrency)
}

func TestDownloadConcurrencyCustom(t *testing.T) {
	conf := &remote_pb.RemoteConf{DownloadConcurrency: 4}
	downloadConcurrency := 1
	if conf.DownloadConcurrency > 0 {
		downloadConcurrency = int(conf.DownloadConcurrency)
	}
	assert.Equal(t, 4, downloadConcurrency)
}

func TestReadTimeoutDefault(t *testing.T) {
	conf := &remote_pb.RemoteConf{}
	readTimeout := int32(300)
	if conf.ReadTimeoutSeconds > 0 {
		readTimeout = conf.ReadTimeoutSeconds
	}
	assert.Equal(t, int32(300), readTimeout)
}

func TestReadTimeoutCustom(t *testing.T) {
	conf := &remote_pb.RemoteConf{ReadTimeoutSeconds: 60}
	readTimeout := int32(300)
	if conf.ReadTimeoutSeconds > 0 {
		readTimeout = conf.ReadTimeoutSeconds
	}
	assert.Equal(t, int32(60), readTimeout)
}
