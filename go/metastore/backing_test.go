package metastore

import (
	"testing"
)

func TestMemoryBacking(t *testing.T) {
	ms := &MetaStore{NewMetaStoreMemoryBacking()}
	verifySetGet(t, ms)
}

func TestFileBacking(t *testing.T) {
	ms := &MetaStore{NewMetaStoreFileBacking()}
	verifySetGet(t, ms)
}

func verifySetGet(t *testing.T, ms *MetaStore) {
	data := uint64(234234)
	ms.SetUint64(data, "/tmp", "sequence")
	if !ms.Has("/tmp", "sequence") {
		t.Errorf("Failed to set data")
	}
	if val, err := ms.GetUint64("/tmp", "sequence"); err == nil {
		if val != data {
			t.Errorf("Set %d, but read back %d", data, val)
		}
	} else {
		t.Errorf("Failed to get back data:%s", err)
	}
}
