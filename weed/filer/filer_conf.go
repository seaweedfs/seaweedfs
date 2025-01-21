package filer

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/viant/ptrie"
	jsonpb "google.golang.org/protobuf/encoding/protojson"
)

const (
	DirectoryEtcRoot      = "/etc/"
	DirectoryEtcSeaweedFS = "/etc/seaweedfs"
	DirectoryEtcRemote    = "/etc/remote"
	FilerConfName         = "filer.conf"
	IamConfigDirectory    = "/etc/iam"
	IamIdentityFile       = "identity.json"
	IamPoliciesFile       = "policies.json"
)

type FilerConf struct {
	rules ptrie.Trie[*filer_pb.FilerConf_PathConf]
}

func ReadFilerConf(filerGrpcAddress pb.ServerAddress, grpcDialOption grpc.DialOption, masterClient *wdclient.MasterClient) (*FilerConf, error) {
	var buf bytes.Buffer
	if err := pb.WithGrpcFilerClient(false, 0, filerGrpcAddress, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		if masterClient != nil {
			return ReadEntry(masterClient, client, DirectoryEtcSeaweedFS, FilerConfName, &buf)
		} else {
			content, err := ReadInsideFiler(client, DirectoryEtcSeaweedFS, FilerConfName)
			buf = *bytes.NewBuffer(content)
			return err
		}
	}); err != nil && err != filer_pb.ErrNotFound {
		return nil, fmt.Errorf("read %s/%s: %v", DirectoryEtcSeaweedFS, FilerConfName, err)
	}

	fc := NewFilerConf()
	if buf.Len() > 0 {
		if err := fc.LoadFromBytes(buf.Bytes()); err != nil {
			return nil, fmt.Errorf("parse %s/%s: %v", DirectoryEtcSeaweedFS, FilerConfName, err)
		}
	}
	return fc, nil
}

func NewFilerConf() (fc *FilerConf) {
	fc = &FilerConf{
		rules: ptrie.New[*filer_pb.FilerConf_PathConf](),
	}
	return fc
}

func (fc *FilerConf) loadFromFiler(filer *Filer) (err error) {
	filerConfPath := util.NewFullPath(DirectoryEtcSeaweedFS, FilerConfName)
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

	return fc.loadFromChunks(filer, entry.Content, entry.GetChunks(), entry.Size())
}

func (fc *FilerConf) loadFromChunks(filer *Filer, content []byte, chunks []*filer_pb.FileChunk, size uint64) (err error) {
	if len(content) == 0 {
		content, err = filer.readEntry(chunks, size)
		if err != nil {
			glog.Errorf("read filer conf content: %v", err)
			return
		}
	}

	return fc.LoadFromBytes(content)
}

func (fc *FilerConf) LoadFromBytes(data []byte) (err error) {
	conf := &filer_pb.FilerConf{}

	if err := jsonpb.Unmarshal(data, conf); err != nil {
		return err
	}

	return fc.doLoadConf(conf)
}

func (fc *FilerConf) doLoadConf(conf *filer_pb.FilerConf) (err error) {
	for _, location := range conf.Locations {
		err = fc.SetLocationConf(location)
		if err != nil {
			// this is not recoverable
			return nil
		}
	}
	return nil
}

func (fc *FilerConf) GetLocationConf(locationPrefix string) (locConf *filer_pb.FilerConf_PathConf, found bool) {
	return fc.rules.Get([]byte(locationPrefix))
}

func (fc *FilerConf) SetLocationConf(locConf *filer_pb.FilerConf_PathConf) (err error) {
	err = fc.rules.Put([]byte(locConf.LocationPrefix), locConf)
	if err != nil {
		glog.Errorf("put location prefix: %v", err)
	}
	return
}

func (fc *FilerConf) AddLocationConf(locConf *filer_pb.FilerConf_PathConf) (err error) {
	existingConf, found := fc.rules.Get([]byte(locConf.LocationPrefix))
	if found {
		mergePathConf(existingConf, locConf)
		locConf = existingConf
	}
	err = fc.rules.Put([]byte(locConf.LocationPrefix), locConf)
	if err != nil {
		glog.Errorf("put location prefix: %v", err)
	}
	return
}

func (fc *FilerConf) DeleteLocationConf(locationPrefix string) {
	rules := ptrie.New[*filer_pb.FilerConf_PathConf]()
	fc.rules.Walk(func(key []byte, value *filer_pb.FilerConf_PathConf) bool {
		if string(key) == locationPrefix {
			return true
		}
		key = bytes.Clone(key)
		_ = rules.Put(key, value)
		return true
	})
	fc.rules = rules
	return
}

func (fc *FilerConf) MatchStorageRule(path string) (pathConf *filer_pb.FilerConf_PathConf) {
	pathConf = &filer_pb.FilerConf_PathConf{}
	fc.rules.MatchPrefix([]byte(path), func(key []byte, value *filer_pb.FilerConf_PathConf) bool {
		mergePathConf(pathConf, value)
		return true
	})
	return pathConf
}

func (fc *FilerConf) GetCollectionTtls(collection string) (ttls map[string]string) {
	ttls = make(map[string]string)
	fc.rules.Walk(func(key []byte, value *filer_pb.FilerConf_PathConf) bool {
		if value.Collection == collection {
			ttls[value.LocationPrefix] = value.GetTtl()
		}
		return true
	})
	return ttls
}

// merge if values in b is not empty, merge them into a
func mergePathConf(a, b *filer_pb.FilerConf_PathConf) {
	a.Collection = util.Nvl(b.Collection, a.Collection)
	a.Replication = util.Nvl(b.Replication, a.Replication)
	a.Ttl = util.Nvl(b.Ttl, a.Ttl)
	a.DiskType = util.Nvl(b.DiskType, a.DiskType)
	a.Fsync = b.Fsync || a.Fsync
	if b.VolumeGrowthCount > 0 {
		a.VolumeGrowthCount = b.VolumeGrowthCount
	}
	a.ReadOnly = b.ReadOnly || a.ReadOnly
	if b.MaxFileNameLength > 0 {
		a.MaxFileNameLength = b.MaxFileNameLength
	}
	a.DataCenter = util.Nvl(b.DataCenter, a.DataCenter)
	a.Rack = util.Nvl(b.Rack, a.Rack)
	a.DataNode = util.Nvl(b.DataNode, a.DataNode)
	a.DisableChunkDeletion = b.DisableChunkDeletion || a.DisableChunkDeletion
	a.Worm = b.Worm || a.Worm
	if b.WormRetentionTimeSeconds > 0 {
		a.WormRetentionTimeSeconds = b.WormRetentionTimeSeconds
	}
	if b.WormGracePeriodSeconds > 0 {
		a.WormGracePeriodSeconds = b.WormGracePeriodSeconds
	}
}

func (fc *FilerConf) ToProto() *filer_pb.FilerConf {
	m := &filer_pb.FilerConf{}
	fc.rules.Walk(func(key []byte, value *filer_pb.FilerConf_PathConf) bool {
		m.Locations = append(m.Locations, value)
		return true
	})
	return m
}

func (fc *FilerConf) ToText(writer io.Writer) error {
	return ProtoToText(writer, fc.ToProto())
}
