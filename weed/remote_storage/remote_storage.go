package remote_storage

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"sync"
)

type VisitFunc func(dir string, name string, isDirectory bool, remoteEntry *filer_pb.RemoteEntry) error

type RemoteStorageClient interface {
	Traverse(rootDir string, visitFn VisitFunc) error
}

type RemoteStorageClientMaker interface {
	Make(remoteConf *filer_pb.RemoteConf) (RemoteStorageClient, error)
}

var (
	RemoteStorageClientMakers = make(map[string]RemoteStorageClientMaker)
	remoteStorageClients      = make(map[string]RemoteStorageClient)
	remoteStorageClientsLock  sync.Mutex
)

func makeRemoteStorageClient(remoteConf *filer_pb.RemoteConf) (RemoteStorageClient, error) {
	maker, found := RemoteStorageClientMakers[remoteConf.Type]
	if !found {
		return nil, fmt.Errorf("remote storage type %s not found", remoteConf.Type)
	}
	return maker.Make(remoteConf)
}

func GetRemoteStorage(remoteConf *filer_pb.RemoteConf) (RemoteStorageClient, error) {
	remoteStorageClientsLock.Lock()
	defer remoteStorageClientsLock.Unlock()

	existingRemoteStorageClient, found := remoteStorageClients[remoteConf.Name]
	if found {
		return existingRemoteStorageClient, nil
	}

	newRemoteStorageClient, err := makeRemoteStorageClient(remoteConf)
	if err != nil {
		return nil, fmt.Errorf("make remote storage client %s: %v", remoteConf.Name, err)
	}

	remoteStorageClients[remoteConf.Name] = newRemoteStorageClient

	return newRemoteStorageClient, nil
}
