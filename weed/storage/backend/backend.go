package backend

import (
	"github.com/seaweedfs/seaweedfs/weed/util"
	"io"
	"os"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

type BackendStorageFile interface {
	io.ReaderAt
	io.WriterAt
	Truncate(off int64) error
	io.Closer
	GetStat() (datSize int64, modTime time.Time, err error)
	Name() string
	Sync() error
}

type BackendStorage interface {
	ToProperties() map[string]string
	NewStorageFile(key string, tierInfo *volume_server_pb.VolumeInfo) BackendStorageFile
	CopyFile(f *os.File, fn func(progressed int64, percentage float32) error) (key string, size int64, err error)
	DownloadFile(fileName string, key string, fn func(progressed int64, percentage float32) error) (size int64, err error)
	DeleteFile(key string) (err error)
}

type StringProperties interface {
	GetString(key string) string
}
type StorageType string
type BackendStorageFactory interface {
	StorageType() StorageType
	BuildStorage(configuration StringProperties, configPrefix string, id string) (BackendStorage, error)
}

var (
	BackendStorageFactories = make(map[StorageType]BackendStorageFactory)
	BackendStorages         = make(map[string]BackendStorage)
)

// used by master to load remote storage configurations
func LoadConfiguration(config *util.ViperProxy) {

	StorageBackendPrefix := "storage.backend"

	for backendTypeName := range config.GetStringMap(StorageBackendPrefix) {
		backendStorageFactory, found := BackendStorageFactories[StorageType(backendTypeName)]
		if !found {
			glog.Fatalf("backend storage type %s not found", backendTypeName)
		}
		for backendStorageId := range config.GetStringMap(StorageBackendPrefix + "." + backendTypeName) {
			if !config.GetBool(StorageBackendPrefix + "." + backendTypeName + "." + backendStorageId + ".enabled") {
				continue
			}
			if _, found := BackendStorages[backendTypeName+"."+backendStorageId]; found {
				continue
			}
			backendStorage, buildErr := backendStorageFactory.BuildStorage(config,
				StorageBackendPrefix+"."+backendTypeName+"."+backendStorageId+".", backendStorageId)
			if buildErr != nil {
				glog.Fatalf("fail to create backend storage %s.%s", backendTypeName, backendStorageId)
			}
			BackendStorages[backendTypeName+"."+backendStorageId] = backendStorage
			if backendStorageId == "default" {
				BackendStorages[backendTypeName] = backendStorage
			}
		}
	}

}

// used by volume server to receive remote storage configurations from master
func LoadFromPbStorageBackends(storageBackends []*master_pb.StorageBackend) {

	for _, storageBackend := range storageBackends {
		backendStorageFactory, found := BackendStorageFactories[StorageType(storageBackend.Type)]
		if !found {
			glog.Warningf("storage type %s not found", storageBackend.Type)
			continue
		}
		if _, found := BackendStorages[storageBackend.Type+"."+storageBackend.Id]; found {
			continue
		}
		backendStorage, buildErr := backendStorageFactory.BuildStorage(newProperties(storageBackend.Properties), "", storageBackend.Id)
		if buildErr != nil {
			glog.Fatalf("fail to create backend storage %s.%s", storageBackend.Type, storageBackend.Id)
		}
		BackendStorages[storageBackend.Type+"."+storageBackend.Id] = backendStorage
		if storageBackend.Id == "default" {
			BackendStorages[storageBackend.Type] = backendStorage
		}
	}
}

type Properties struct {
	m map[string]string
}

func newProperties(m map[string]string) *Properties {
	return &Properties{m: m}
}

func (p *Properties) GetString(key string) string {
	if v, found := p.m[key]; found {
		return v
	}
	return ""
}

func ToPbStorageBackends() (backends []*master_pb.StorageBackend) {
	for sName, s := range BackendStorages {
		sType, sId := BackendNameToTypeId(sName)
		if sType == "" {
			continue
		}
		backends = append(backends, &master_pb.StorageBackend{
			Type:       sType,
			Id:         sId,
			Properties: s.ToProperties(),
		})
	}
	return
}

func BackendNameToTypeId(backendName string) (backendType, backendId string) {
	parts := strings.Split(backendName, ".")
	if len(parts) == 1 {
		return backendName, "default"
	}
	if len(parts) != 2 {
		return
	}

	backendType, backendId = parts[0], parts[1]
	return
}
