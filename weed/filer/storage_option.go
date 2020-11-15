package filer

import "github.com/chrislusf/seaweedfs/weed/storage/needle"

type StorageOption struct {
	Replication string
	Collection  string
	DataCenter  string
	Rack        string
	TtlSeconds  int32
	Fsync       bool
}

func (so *StorageOption) TtlString() string {
	return needle.SecondsToTTL(so.TtlSeconds)
}