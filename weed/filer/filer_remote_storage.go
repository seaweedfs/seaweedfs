package filer

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/remote_storage"
	_ "github.com/chrislusf/seaweedfs/weed/remote_storage/s3"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/golang/protobuf/proto"
	"math"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/viant/ptrie"
)

const REMOTE_STORAGE_CONF_SUFFIX = ".conf"
const REMOTE_STORAGE_MOUNT_FILE = "mount.mapping"

type FilerRemoteStorage struct {
	rules             ptrie.Trie
	storageNameToConf map[string]*filer_pb.RemoteConf
}

func NewFilerRemoteStorage() (rs *FilerRemoteStorage) {
	rs = &FilerRemoteStorage{
		rules:             ptrie.New(),
		storageNameToConf: make(map[string]*filer_pb.RemoteConf),
	}
	return rs
}

func (rs *FilerRemoteStorage) LoadRemoteStorageConfigurationsAndMapping(filer *Filer) (err error) {
	// execute this on filer

	entries, _, err := filer.ListDirectoryEntries(context.Background(), DirectoryEtcRemote, "", false, math.MaxInt64, "", "", "")
	if err != nil {
		if err == filer_pb.ErrNotFound {
			return nil
		}
		glog.Errorf("read remote storage %s: %v", DirectoryEtcRemote, err)
		return
	}

	for _, entry := range entries {
		if entry.Name() == REMOTE_STORAGE_MOUNT_FILE {
			if err := rs.loadRemoteStorageMountMapping(entry.Content); err != nil {
				return err
			}
			continue
		}
		if !strings.HasSuffix(entry.Name(), REMOTE_STORAGE_CONF_SUFFIX) {
			return nil
		}
		conf := &filer_pb.RemoteConf{}
		if err := proto.Unmarshal(entry.Content, conf); err != nil {
			return fmt.Errorf("unmarshal %s/%s: %v", DirectoryEtcRemote, entry.Name(), err)
		}
		rs.storageNameToConf[conf.Name] = conf
	}
	return nil
}

func (rs *FilerRemoteStorage) loadRemoteStorageMountMapping(data []byte) (err error) {
	mappings := &filer_pb.RemoteStorageMapping{}
	if err := proto.Unmarshal(data, mappings); err != nil {
		return fmt.Errorf("unmarshal %s/%s: %v", DirectoryEtcRemote, REMOTE_STORAGE_MOUNT_FILE, err)
	}
	for dir, storageLocation := range mappings.Mappings {
		rs.mapDirectoryToRemoteStorage(util.FullPath(dir), storageLocation)
	}
	return nil
}

func (rs *FilerRemoteStorage) mapDirectoryToRemoteStorage(dir util.FullPath, loc *filer_pb.RemoteStorageLocation) {
	rs.rules.Put([]byte(dir+"/"), loc)
}

func (rs *FilerRemoteStorage) FindMountDirectory(p util.FullPath) (mountDir util.FullPath, remoteLocation *filer_pb.RemoteStorageLocation) {
	rs.rules.MatchPrefix([]byte(p), func(key []byte, value interface{}) bool {
		mountDir = util.FullPath(string(key[:len(key)-1]))
		remoteLocation = value.(*filer_pb.RemoteStorageLocation)
		return true
	})
	return
}

func (rs *FilerRemoteStorage) FindRemoteStorageClient(p util.FullPath) (client remote_storage.RemoteStorageClient, remoteConf *filer_pb.RemoteConf, found bool) {
	var storageLocation *filer_pb.RemoteStorageLocation
	rs.rules.MatchPrefix([]byte(p), func(key []byte, value interface{}) bool {
		storageLocation = value.(*filer_pb.RemoteStorageLocation)
		return true
	})

	if storageLocation == nil {
		found = false
		return
	}

	return rs.GetRemoteStorageClient(storageLocation.Name)
}

func (rs *FilerRemoteStorage) GetRemoteStorageClient(storageName string) (client remote_storage.RemoteStorageClient, remoteConf *filer_pb.RemoteConf, found bool) {
	remoteConf, found = rs.storageNameToConf[storageName]
	if !found {
		return
	}

	var err error
	if client, err = remote_storage.GetRemoteStorage(remoteConf); err == nil {
		found = true
		return
	}
	return
}

func UnmarshalRemoteStorageMappings(oldContent []byte) (mappings *filer_pb.RemoteStorageMapping, err error) {
	mappings = &filer_pb.RemoteStorageMapping{
		Mappings: make(map[string]*filer_pb.RemoteStorageLocation),
	}
	if len(oldContent) > 0 {
		if err = proto.Unmarshal(oldContent, mappings); err != nil {
			glog.Warningf("unmarshal existing mappings: %v", err)
		}
	}
	return
}

func AddRemoteStorageMapping(oldContent []byte, dir string, storageLocation *filer_pb.RemoteStorageLocation) (newContent []byte, err error) {
	mappings, unmarshalErr := UnmarshalRemoteStorageMappings(oldContent)
	if unmarshalErr != nil {
		// skip
	}

	// set the new mapping
	mappings.Mappings[dir] = storageLocation

	if newContent, err = proto.Marshal(mappings); err != nil {
		return oldContent, fmt.Errorf("marshal mappings: %v", err)
	}

	return
}