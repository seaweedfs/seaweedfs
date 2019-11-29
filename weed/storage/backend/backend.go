package backend

import (
	"io"
	"os"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/spf13/viper"
)

type BackendStorageFile interface {
	io.ReaderAt
	io.WriterAt
	Truncate(off int64) error
	io.Closer
	GetStat() (datSize int64, modTime time.Time, err error)
	String() string
	Instantiate(src *os.File) error
}

type BackendStorage interface {
	Name() string
	NewStorageFile(key string) BackendStorageFile
}

type StorageType string
type BackendStorageFactory interface {
	StorageType() StorageType
	BuildStorage(configuration util.Configuration, id string) (BackendStorage, error)
}

var (
	BackendStorageFactories = make(map[StorageType]BackendStorageFactory)
	BackendStorages         = make(map[string]BackendStorage)
)

func LoadConfiguration(config *viper.Viper) {

	StorageBackendPrefix := "storage.backend"

	backendSub := config.Sub(StorageBackendPrefix)

	for backendTypeName, _ := range config.GetStringMap(StorageBackendPrefix) {
		backendStorageFactory, found := BackendStorageFactories[StorageType(backendTypeName)]
		if !found {
			glog.Fatalf("backend storage type %s not found", backendTypeName)
		}
		backendTypeSub := backendSub.Sub(backendTypeName)
		for backendStorageId, _ := range backendSub.GetStringMap(backendTypeName) {
			backendStorage, buildErr := backendStorageFactory.BuildStorage(backendTypeSub.Sub(backendStorageId), backendStorageId)
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
