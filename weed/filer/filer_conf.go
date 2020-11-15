package filer

import (
	"context"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/golang/protobuf/proto"
	"github.com/viant/ptrie"
)


const (
	DirectoryEtc = "/etc"
	FilerConfName = "filer.conf"
)

type FilerConf struct {
	rules ptrie.Trie
}

func NewFilerConf() (fc *FilerConf) {
	fc = &FilerConf{
		rules: ptrie.New(),
	}
	return fc
}

func (fc *FilerConf) loadFromFiler(filer *Filer) (err error){
	filerConfPath := util.NewFullPath(DirectoryEtc, FilerConfName)
	entry, err := filer.FindEntry(context.Background(), filerConfPath)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			return nil
		}
		glog.Errorf("read filer conf entry %s: %v", filerConfPath, err)
		return
	}

	return fc.loadFromChunks(filer, entry.Chunks)
}

func (fc *FilerConf) loadFromChunks(filer *Filer, chunks []*filer_pb.FileChunk) (err error) {
	data, err := filer.readEntry(chunks)
	if err != nil {
		glog.Errorf("read filer conf content: %v", err)
		return
	}

	return fc.loadFromBytes(data)
}

func (fc *FilerConf) loadFromBytes(data []byte) (err error) {
	conf := &filer_pb.FilerConf{}
	err = proto.UnmarshalText(string(data), conf)
	if err != nil {
		glog.Errorf("unable to parse filer conf: %v", err)
		// this is not recoverable
		return nil
	}

	return fc.doLoadConf(conf)
}

func (fc *FilerConf) doLoadConf(conf *filer_pb.FilerConf) (err error) {
	for _, location := range conf.Locations {
		err = fc.rules.Put([]byte(location.LocationPrefix), location)
		if err != nil {
			glog.Errorf("put location prefix: %v", err)
			// this is not recoverable
			return nil
		}
	}
	return nil
}

var (
	EmptyFilerConfPathConf = &filer_pb.FilerConf_PathConf{}
)

func (fc *FilerConf) MatchStorageRule(path string) (pathConf *filer_pb.FilerConf_PathConf){
	fc.rules.MatchPrefix([]byte(path), func(key []byte, value interface{}) bool {
		pathConf = value.(*filer_pb.FilerConf_PathConf)
		return true
	})
	if pathConf == nil {
		return EmptyFilerConfPathConf
	}
	return pathConf
}
