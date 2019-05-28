package storage

import (
	"testing"
)

func TestLoadingEcShards(t *testing.T) {
	dl := NewDiskLocation("./erasure_coding", 100)
	err := dl.loadAllEcShards()
	if err != nil {
		t.Errorf("load all ec shards: %v", err)
	}

	if len(dl.ecVolumes)!=1 {
		t.Errorf("loading err")
	}
}