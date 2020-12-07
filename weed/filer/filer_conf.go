package filer

import (
	"bytes"
	"context"
	"io"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/golang/protobuf/jsonpb"
	"github.com/viant/ptrie"
)

const (
	DirectoryEtc  = "/etc"
	FilerConfName = "filer.conf"
	IamConfigDirecotry = "/etc/iam"
	IamIdentityFile = "identity.json"
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

func (fc *FilerConf) loadFromFiler(filer *Filer) (err error) {
	filerConfPath := util.NewFullPath(DirectoryEtc, FilerConfName)
	entry, err := filer.FindEntry(context.Background(), filerConfPath)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			return nil
		}
		glog.Errorf("read filer conf entry %s: %v", filerConfPath, err)
		return
	}

	if len(entry.Content) > 0 {
		return fc.LoadFromBytes(entry.Content)
	}

	return fc.loadFromChunks(filer, entry.Chunks)
}

func (fc *FilerConf) loadFromChunks(filer *Filer, chunks []*filer_pb.FileChunk) (err error) {
	data, err := filer.readEntry(chunks)
	if err != nil {
		glog.Errorf("read filer conf content: %v", err)
		return
	}

	return fc.LoadFromBytes(data)
}

func (fc *FilerConf) LoadFromBytes(data []byte) (err error) {
	conf := &filer_pb.FilerConf{}

	if err := jsonpb.Unmarshal(bytes.NewReader(data), conf); err != nil {
		return err
	}

	return fc.doLoadConf(conf)
}

func (fc *FilerConf) doLoadConf(conf *filer_pb.FilerConf) (err error) {
	for _, location := range conf.Locations {
		err = fc.AddLocationConf(location)
		if err != nil {
			// this is not recoverable
			return nil
		}
	}
	return nil
}

func (fc *FilerConf) AddLocationConf(locConf *filer_pb.FilerConf_PathConf) (err error) {
	err = fc.rules.Put([]byte(locConf.LocationPrefix), locConf)
	if err != nil {
		glog.Errorf("put location prefix: %v", err)
	}
	return
}

func (fc *FilerConf) DeleteLocationConf(locationPrefix string) {
	rules := ptrie.New()
	fc.rules.Walk(func(key []byte, value interface{}) bool {
		if string(key) == locationPrefix {
			return true
		}
		rules.Put(key, value)
		return true
	})
	fc.rules = rules
	return
}

func (fc *FilerConf) MatchStorageRule(path string) (pathConf *filer_pb.FilerConf_PathConf) {
	pathConf = &filer_pb.FilerConf_PathConf{}
	fc.rules.MatchPrefix([]byte(path), func(key []byte, value interface{}) bool {
		t := value.(*filer_pb.FilerConf_PathConf)
		mergePathConf(pathConf, t)
		return true
	})
	return pathConf
}

// merge if values in b is not empty, merge them into a
func mergePathConf(a, b *filer_pb.FilerConf_PathConf) {
	a.Collection = util.Nvl(b.Collection, a.Collection)
	a.Replication = util.Nvl(b.Replication, a.Replication)
	a.Ttl = util.Nvl(b.Ttl, a.Ttl)
	if b.DiskType != filer_pb.FilerConf_PathConf_NONE {
		a.DiskType = b.DiskType
	}
	a.Fsync = b.Fsync || a.Fsync
	if b.VolumeGrowthCount > 0 {
		a.VolumeGrowthCount = b.VolumeGrowthCount
	}
}

func (fc *FilerConf) ToProto() *filer_pb.FilerConf {
	m := &filer_pb.FilerConf{}
	fc.rules.Walk(func(key []byte, value interface{}) bool {
		pathConf := value.(*filer_pb.FilerConf_PathConf)
		m.Locations = append(m.Locations, pathConf)
		return true
	})
	return m
}

func (fc *FilerConf) ToText(writer io.Writer) error {

	m := jsonpb.Marshaler{
		EmitDefaults: false,
		Indent:       "  ",
	}

	return m.Marshal(writer, fc.ToProto())
}
