package http

import (
	"sync"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

var (
	volumeHttpClient initObj
	masterHttpClient initObj
	filerHttpClient initObj
	globalHttpClient initObj
)

type initObj struct {
	obj interface{}
	err error
	once sync.Once
}
func (init *initObj) initOnce(initOnceF func()(interface{}, error)) (interface{}, error) {
	init.once.Do(func(){
		init.obj, init.err = initOnceF()
	})
	return init.obj, init.err
}

func GetVolumeHttpClient() (*VolumeHttpClient) {
	client, err := volumeHttpClient.initOnce(
		func() (interface{}, error) {
			return NewVolumeHttpClient()
		},
	)
	if err != nil {
		glog.Fatalf("Error init VolumeHttpClient:`%s`", err)
		return nil
	}
	return client.(*VolumeHttpClient)
}

func GetMasterHttpClient() (*MasterHttpClient) {
	client, err := masterHttpClient.initOnce(
		func() (interface{}, error) {
			return NewMasterHttpClient()
		},
	)
	if err != nil {
		glog.Fatalf("Error init MasterHttpClient:`%s`", err)
		return nil
	}
	return client.(*MasterHttpClient)
}

func GetFilerHttpClient() (*FilerHttpClient) {
	client, err := filerHttpClient.initOnce(
		func() (interface{}, error) {
			return NewFilerHttpClient()
		},
	)
	if err != nil {
		glog.Fatalf("Error init FilerHttpClient:`%s`", err)
		return nil
	}
	return client.(*FilerHttpClient)
}

func GetGlobalHttpClient() (*GlobalHttpClient) {
	client, err := globalHttpClient.initOnce(
		func() (interface{}, error) {
			return NewGlobalHttpClient()
		},
	)
	if err != nil {
		glog.Fatalf("Error init GlobalHttpClient:`%s`", err)
		return nil
	}
	return client.(*GlobalHttpClient)
}
