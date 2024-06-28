package http

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

var (
	filerHttpClient *HTTPClient
	volumeHttpClient *HTTPClient
	masterHttpClient *HTTPClient
)

func GetFilerHttpClient() *HTTPClient {
	return filerHttpClient
}

func GetVolumeHttpClient() *HTTPClient {
	return volumeHttpClient
}

func GetMasterHttpClient() *HTTPClient {
	return masterHttpClient
}

func InitAllHttpClients() {
	var err error
	filerHttpClient, err = NewFilerHttpClient()
	if err != nil {
		glog.Fatalf("error init filer http client: %v", err)
	}
	volumeHttpClient, err = NewVolumeHttpClient()
	if err != nil {
		glog.Fatalf("error init volume http client: %v", err)
	}
	masterHttpClient, err = NewMasterHttpClient()
	if err != nil {
		glog.Fatalf("error init master http client: %v", err)
	}
}
