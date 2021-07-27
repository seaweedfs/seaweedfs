package filer

import (
	"bytes"
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"io"
	"math"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/golang/protobuf/jsonpb"
	"github.com/viant/ptrie"
)

type FilerRemoteStorage struct {
	rules ptrie.Trie
}

func NewFilerRemoteStorage() (fc *FilerRemoteStorage) {
	fc = &FilerRemoteStorage{
		rules: ptrie.New(),
	}
	return fc
}

func (fc *FilerRemoteStorage) loadFromFiler(filer *Filer) (err error) {
	entries, _, err := filer.ListDirectoryEntries(context.Background(), DirectoryEtcRemote, "", false, math.MaxInt64, "", "", "")
	if err != nil {
		if err == filer_pb.ErrNotFound {
			return nil
		}
		glog.Errorf("read remote storage %s: %v", DirectoryEtcRemote, err)
		return
	}

	for _, entry := range entries {
		conf := &filer_pb.RemoteConf{}
		if err := proto.Unmarshal(entry.Content, conf); err != nil {
			return fmt.Errorf("unmarshal %s/%s: %v", DirectoryEtcRemote, entry.Name, err)
		}
		fc.MountRemoteStorage(dir, conf)
	}
	return nil
}
