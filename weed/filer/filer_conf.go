package filer

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/golang/protobuf/proto"
	"github.com/viant/ptrie"
)

type FilerConf struct {
	rules ptrie.Trie
}

func NewFilerConf(data []byte) (fc *FilerConf) {
	fc = &FilerConf{
		rules: ptrie.New(),
	}

	conf := &filer_pb.FilerConf{}
	err := proto.UnmarshalText(string(data), conf)
	if err != nil {
		glog.Errorf("unable to parse filer conf: %v", err)
		return
	}

	fc.doLoadConf(conf)
	return fc
}

func (fc *FilerConf) doLoadConf(conf *filer_pb.FilerConf) {
	for _, location := range conf.Locations {
		err := fc.rules.Put([]byte(location.LocationPrefix), location)
		if err != nil {
			glog.Errorf("put location prefix: %v", err)
		}
	}
}

func (fc *FilerConf) MatchStorageRule(path string) (pathConf *filer_pb.FilerConf_PathConf){
	fc.rules.MatchPrefix([]byte(path), func(key []byte, value interface{}) bool {
		pathConf = value.(*filer_pb.FilerConf_PathConf)
		return true
	})
	if pathConf == nil {
		return &filer_pb.FilerConf_PathConf{}
	}
	return pathConf
}
