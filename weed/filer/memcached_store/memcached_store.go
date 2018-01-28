package memcached_store

import (
	memcache "github.com/bradfitz/gomemcache/memcache"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
)

type MemcacheStore struct {
	Client *memcache.Client
}

func NewMemcacheStore(hostPort ...string) *MemcacheStore {
	client := memcache.New(hostPort...)
	return &MemcacheStore{Client: client}
}

func (s *MemcacheStore) Get(fullFileName string) (fid string, err error) {
	item, err := s.Client.Get(fullFileName)
	if err == memcache.ErrCacheMiss {
		err = filer.ErrNotFound
	}
	if err != nil {
		return "", err
	}
	fid = string(item.Value)
	return fid, err
}

func (s *MemcacheStore) Put(fullFileName string, fid string) (err error) {
	item := memcache.Item{
		Key:   fullFileName,
		Value: []byte(fid),
	}
	err = s.Client.Set(&item)
	return err
}

func (s *MemcacheStore) Delete(fullFileName string) (err error) {
	err = s.Client.Delete(fullFileName)
	// ErrCacheMiss means key is not cached in memcache
	// which is the same as deleted
	// so it should not be consider as an error
	if err == memcache.ErrCacheMiss {
		glog.V(0).Infof("Full file name %s not exists in memcache", fullFileName)
		err = nil
	}
	return err
}

func (s *MemcacheStore) Close() {
	// For now the memcache client do not provide a way to close client
	// and all cleanup job depends on GC
	// related issue: https://github.com/bradfitz/gomemcache/issues/51
	s.Client = nil
}
