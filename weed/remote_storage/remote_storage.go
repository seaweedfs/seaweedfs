package remote_storage

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"io"
	"strings"
	"sync"
)

func ParseLocation(remote string) (loc *filer_pb.RemoteStorageLocation) {
	loc = &filer_pb.RemoteStorageLocation{}
	if strings.HasSuffix(string(remote), "/") {
		remote = remote[:len(remote)-1]
	}
	parts := strings.SplitN(string(remote), "/", 3)
	if len(parts) >= 1 {
		loc.Name = parts[0]
	}
	if len(parts) >= 2 {
		loc.Bucket = parts[1]
	}
	loc.Path = string(remote[len(loc.Name)+1+len(loc.Bucket):])
	if loc.Path == "" {
		loc.Path = "/"
	}
	return
}

type VisitFunc func(dir string, name string, isDirectory bool, remoteEntry *filer_pb.RemoteEntry) error

type RemoteStorageClient interface {
	Traverse(loc *filer_pb.RemoteStorageLocation, visitFn VisitFunc) error
	ReadFile(loc *filer_pb.RemoteStorageLocation, offset int64, size int64) (data []byte, err error)
	WriteDirectory(loc *filer_pb.RemoteStorageLocation, entry *filer_pb.Entry) (err error)
	WriteFile(loc *filer_pb.RemoteStorageLocation, entry *filer_pb.Entry, reader io.Reader) (remoteEntry *filer_pb.RemoteEntry, err error)
	UpdateFileMetadata(loc *filer_pb.RemoteStorageLocation, entry *filer_pb.Entry) (err error)
	DeleteFile(loc *filer_pb.RemoteStorageLocation) (err error)
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
