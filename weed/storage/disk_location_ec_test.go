package storage

import (
	"io/ioutil"
	"testing"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

func TestLoadingEcShards(t *testing.T) {
	dl := NewDiskLocation("./erasure_coding", 100)
	err := dl.loadAllEcShards()
	if err != nil {
		t.Errorf("load all ec shards: %v", err)
	}

	if len(dl.ecVolumes) != 1 {
		t.Errorf("loading err")
	}

	fileInfos, err := ioutil.ReadDir(dl.Directory)
	if err != nil {
		t.Errorf("listing all ec shards in dir %s: %v", dl.Directory, err)
	}

	glog.V(0).Infof("FileCount %d", len(fileInfos))
	for i, fileInfo := range fileInfos {
		glog.V(0).Infof("file:%d %s size:%d", i, fileInfo.Name(), fileInfo.Size())
	}

}
