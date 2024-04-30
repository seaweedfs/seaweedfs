package filer

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"math"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/viant/ptrie"
)

const REMOTE_STORAGE_CONF_SUFFIX = ".conf"
const REMOTE_STORAGE_MOUNT_FILE = "mount.mapping"

type FilerRemoteStorage struct {
	rules             ptrie.Trie[*remote_pb.RemoteStorageLocation]
	storageNameToConf map[string]*remote_pb.RemoteConf
}

func NewFilerRemoteStorage() (rs *FilerRemoteStorage) {
	rs = &FilerRemoteStorage{
		rules:             ptrie.New[*remote_pb.RemoteStorageLocation](),
		storageNameToConf: make(map[string]*remote_pb.RemoteConf),
	}
	return rs
}

func (rs *FilerRemoteStorage) LoadRemoteStorageConfigurationsAndMapping(filer *Filer) (err error) {
	// execute this on filer

	limit := int64(math.MaxInt32)

	entries, _, err := filer.ListDirectoryEntries(context.Background(), DirectoryEtcRemote, "", false, limit, "", "", "")
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
		conf := &remote_pb.RemoteConf{}
		if err := proto.Unmarshal(entry.Content, conf); err != nil {
			return fmt.Errorf("unmarshal %s/%s: %v", DirectoryEtcRemote, entry.Name(), err)
		}
		rs.storageNameToConf[conf.Name] = conf
	}
	return nil
}

func (rs *FilerRemoteStorage) loadRemoteStorageMountMapping(data []byte) (err error) {
	mappings := &remote_pb.RemoteStorageMapping{}
	if err := proto.Unmarshal(data, mappings); err != nil {
		return fmt.Errorf("unmarshal %s/%s: %v", DirectoryEtcRemote, REMOTE_STORAGE_MOUNT_FILE, err)
	}
	for dir, storageLocation := range mappings.Mappings {
		rs.mapDirectoryToRemoteStorage(util.FullPath(dir), storageLocation)
	}
	return nil
}

func (rs *FilerRemoteStorage) mapDirectoryToRemoteStorage(dir util.FullPath, loc *remote_pb.RemoteStorageLocation) {
	rs.rules.Put([]byte(dir+"/"), loc)
}

func (rs *FilerRemoteStorage) FindMountDirectory(p util.FullPath) (mountDir util.FullPath, remoteLocation *remote_pb.RemoteStorageLocation) {
	rs.rules.MatchPrefix([]byte(p), func(key []byte, value *remote_pb.RemoteStorageLocation) bool {
		mountDir = util.FullPath(string(key[:len(key)-1]))
		remoteLocation = value
		return true
	})
	return
}

func (rs *FilerRemoteStorage) FindRemoteStorageClient(p util.FullPath) (client remote_storage.RemoteStorageClient, remoteConf *remote_pb.RemoteConf, found bool) {
	var storageLocation *remote_pb.RemoteStorageLocation
	rs.rules.MatchPrefix([]byte(p), func(key []byte, value *remote_pb.RemoteStorageLocation) bool {
		storageLocation = value
		return true
	})

	if storageLocation == nil {
		found = false
		return
	}

	return rs.GetRemoteStorageClient(storageLocation.Name)
}

func (rs *FilerRemoteStorage) GetRemoteStorageClient(storageName string) (client remote_storage.RemoteStorageClient, remoteConf *remote_pb.RemoteConf, found bool) {
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

func UnmarshalRemoteStorageMappings(oldContent []byte) (mappings *remote_pb.RemoteStorageMapping, err error) {
	mappings = &remote_pb.RemoteStorageMapping{
		Mappings: make(map[string]*remote_pb.RemoteStorageLocation),
	}
	if len(oldContent) > 0 {
		if err = proto.Unmarshal(oldContent, mappings); err != nil {
			glog.Warningf("unmarshal existing mappings: %v", err)
		}
	}
	return
}

func ReadRemoteStorageConf(grpcDialOption grpc.DialOption, filerAddress pb.ServerAddress, storageName string) (conf *remote_pb.RemoteConf, readErr error) {
	var oldContent []byte
	if readErr = pb.WithFilerClient(false, 0, filerAddress, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		oldContent, readErr = ReadInsideFiler(client, DirectoryEtcRemote, storageName+REMOTE_STORAGE_CONF_SUFFIX)
		return readErr
	}); readErr != nil {
		return nil, readErr
	}

	// unmarshal storage configuration
	conf = &remote_pb.RemoteConf{}
	if unMarshalErr := proto.Unmarshal(oldContent, conf); unMarshalErr != nil {
		readErr = fmt.Errorf("unmarshal %s/%s: %v", DirectoryEtcRemote, storageName+REMOTE_STORAGE_CONF_SUFFIX, unMarshalErr)
		return
	}

	return
}

func DetectMountInfo(grpcDialOption grpc.DialOption, filerAddress pb.ServerAddress, dir string) (*remote_pb.RemoteStorageMapping, string, *remote_pb.RemoteStorageLocation, *remote_pb.RemoteConf, error) {

	mappings, listErr := ReadMountMappings(grpcDialOption, filerAddress)
	if listErr != nil {
		return nil, "", nil, nil, listErr
	}
	if dir == "" {
		return mappings, "", nil, nil, fmt.Errorf("need to specify '-dir' option")
	}

	var localMountedDir string
	var remoteStorageMountedLocation *remote_pb.RemoteStorageLocation
	for k, loc := range mappings.Mappings {
		if strings.HasPrefix(dir, k) {
			localMountedDir, remoteStorageMountedLocation = k, loc
		}
	}
	if localMountedDir == "" {
		return mappings, localMountedDir, remoteStorageMountedLocation, nil, fmt.Errorf("%s is not mounted", dir)
	}

	// find remote storage configuration
	remoteStorageConf, err := ReadRemoteStorageConf(grpcDialOption, filerAddress, remoteStorageMountedLocation.Name)
	if err != nil {
		return mappings, localMountedDir, remoteStorageMountedLocation, remoteStorageConf, err
	}

	return mappings, localMountedDir, remoteStorageMountedLocation, remoteStorageConf, nil
}
