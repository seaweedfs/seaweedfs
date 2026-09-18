package filer

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/viant/ptrie"
)

const REMOTE_STORAGE_CONF_SUFFIX = ".conf"
const REMOTE_STORAGE_MOUNT_FILE = "mount.mapping"

// RemoteStorageConfValidator rejects a RemoteConf whose endpoint resolves to
// an address the filer must not dial (loopback / link-local / private / IMDS).
// The filer server injects the volume server's SSRF deny-list validator so the
// filer package — which cannot import the server package — applies the same
// check the volume server's BuildGuardedRemoteStorageClient does. A nil
// validator leaves the historical unguarded behavior for unit tests that
// never dial.
type RemoteStorageConfValidator func(ctx context.Context, conf *remote_pb.RemoteConf) error

type FilerRemoteStorage struct {
	// guards rules and storageNameToConf, which are replaced wholesale
	// whenever /etc/remote changes
	mu                sync.RWMutex
	rules             ptrie.Trie[*remote_pb.RemoteStorageLocation]
	storageNameToConf map[string]*remote_pb.RemoteConf
	// confValidator, when set, is applied to every RemoteConf as it is loaded
	// from /etc/remote. A conf that fails is dropped from storageNameToConf so
	// the lazy-fetch, lazy-list, and remote-delete paths (which resolve clients
	// by name) can never dial its endpoint. This closes the unauthenticated-plant
	// SSRF: a conf written to /etc/remote with a loopback S3 endpoint is rejected
	// at reload instead of being dialed on the next cache miss.
	confValidator RemoteStorageConfValidator
}

// RemoteStorageClientBuilder builds a remote-storage client for a conf. The
// filer server sets it to the guarded builder (endpoint deny-list + DNS
// rebinding-safe dialer) so the lazy-remote paths apply the same SSRF checks
// as the volume and streaming read paths. When nil the lazy paths fall back to
// the shared unguarded cache.
type RemoteStorageClientBuilder func(ctx context.Context, remoteConf *remote_pb.RemoteConf, allowUntrusted bool) (remote_storage.RemoteStorageClient, error)

func NewFilerRemoteStorage() (rs *FilerRemoteStorage) {
	rs = &FilerRemoteStorage{
		rules:             ptrie.New[*remote_pb.RemoteStorageLocation](),
		storageNameToConf: make(map[string]*remote_pb.RemoteConf),
	}
	return rs
}

// SetConfValidator installs the SSRF deny-list validator applied to every
// RemoteConf loaded from /etc/remote. It is called once by the filer server
// after construction.
func (rs *FilerRemoteStorage) SetConfValidator(v RemoteStorageConfValidator) {
	rs.mu.Lock()
	rs.confValidator = v
	rs.mu.Unlock()
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

	// build into fresh containers so an unmounted directory disappears instead
	// of lingering in the trie, which has no way to drop a key
	rules := ptrie.New[*remote_pb.RemoteStorageLocation]()
	storageNameToConf := make(map[string]*remote_pb.RemoteConf)

	for _, entry := range entries {
		if entry.Name() == REMOTE_STORAGE_MOUNT_FILE {
			if err := loadRemoteStorageMountMapping(rules, entry.Content); err != nil {
				return err
			}
			continue
		}
		if !strings.HasSuffix(entry.Name(), REMOTE_STORAGE_CONF_SUFFIX) {
			continue
		}
		conf := &remote_pb.RemoteConf{}
		if err := proto.Unmarshal(entry.Content, conf); err != nil {
			return fmt.Errorf("unmarshal %s/%s: %v", DirectoryEtcRemote, entry.Name(), err)
		}
		if rs.confValidator != nil {
			if vErr := rs.confValidator(context.Background(), conf); vErr != nil {
				// Drop the conf rather than fail the whole load: a single bad conf
				// must not evict the rest of /etc/remote, and the mount mapping that
				// references it resolves to "no client" on the lazy paths instead
				// of dialing the blocked endpoint.
				glog.Warningf("reject remote storage conf %s/%s: %v", DirectoryEtcRemote, entry.Name(), vErr)
				continue
			}
		}
		storageNameToConf[conf.Name] = conf
	}

	rs.mu.Lock()
	rs.rules, rs.storageNameToConf = rules, storageNameToConf
	rs.mu.Unlock()

	return nil
}

func loadRemoteStorageMountMapping(rules ptrie.Trie[*remote_pb.RemoteStorageLocation], data []byte) (err error) {
	mappings := &remote_pb.RemoteStorageMapping{}
	if err := proto.Unmarshal(data, mappings); err != nil {
		return fmt.Errorf("unmarshal %s/%s: %v", DirectoryEtcRemote, REMOTE_STORAGE_MOUNT_FILE, err)
	}
	for dir, storageLocation := range mappings.Mappings {
		putDirectoryToRemoteStorage(rules, util.FullPath(dir), storageLocation)
	}
	return nil
}

func (rs *FilerRemoteStorage) mapDirectoryToRemoteStorage(dir util.FullPath, loc *remote_pb.RemoteStorageLocation) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	putDirectoryToRemoteStorage(rs.rules, dir, loc)
}

func putDirectoryToRemoteStorage(rules ptrie.Trie[*remote_pb.RemoteStorageLocation], dir util.FullPath, loc *remote_pb.RemoteStorageLocation) {
	rules.Put([]byte(dir+"/"), loc)
}

// FindMountDirectory returns the mount directory and location for p. When multiple
// mounts match (e.g. /buckets/b and /buckets/b/prefix), ptrie MatchPrefix visits
// shorter prefixes first, so the last match is the longest prefix.
func (rs *FilerRemoteStorage) FindMountDirectory(p util.FullPath) (mountDir util.FullPath, remoteLocation *remote_pb.RemoteStorageLocation) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	rs.rules.MatchPrefix([]byte(p), func(key []byte, value *remote_pb.RemoteStorageLocation) bool {
		mountDir = util.FullPath(string(key[:len(key)-1]))
		remoteLocation = value
		return true
	})
	return
}

func (rs *FilerRemoteStorage) FindRemoteStorageClient(p util.FullPath) (client remote_storage.RemoteStorageClient, remoteConf *remote_pb.RemoteConf, found bool) {
	_, storageLocation := rs.FindMountDirectory(p)
	if storageLocation == nil {
		return nil, nil, false
	}

	return rs.GetRemoteStorageClient(storageLocation.Name)
}

func (rs *FilerRemoteStorage) GetRemoteStorageClient(storageName string) (client remote_storage.RemoteStorageClient, remoteConf *remote_pb.RemoteConf, found bool) {
	rs.mu.RLock()
	remoteConf, found = rs.storageNameToConf[storageName]
	rs.mu.RUnlock()
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

func (rs *FilerRemoteStorage) FindRemoteStorageConf(p util.FullPath) (*remote_pb.RemoteConf, bool) {
	_, storageLocation := rs.FindMountDirectory(p)
	if storageLocation == nil {
		return nil, false
	}
	return rs.GetRemoteStorageConf(storageLocation.Name)
}

func (rs *FilerRemoteStorage) GetRemoteStorageConf(storageName string) (*remote_pb.RemoteConf, bool) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	conf, found := rs.storageNameToConf[storageName]
	return conf, found
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
		oldContent, readErr = ReadInsideFiler(context.Background(), client, DirectoryEtcRemote, storageName+REMOTE_STORAGE_CONF_SUFFIX)
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

	localMountedDir, remoteStorageMountedLocation, findErr := FindMountedRemoteMapping(mappings, dir)
	if findErr != nil {
		return mappings, localMountedDir, remoteStorageMountedLocation, nil, findErr
	}

	// find remote storage configuration
	remoteStorageConf, err := ReadRemoteStorageConf(grpcDialOption, filerAddress, remoteStorageMountedLocation.Name)
	if err != nil {
		return mappings, localMountedDir, remoteStorageMountedLocation, remoteStorageConf, err
	}

	return mappings, localMountedDir, remoteStorageMountedLocation, remoteStorageConf, nil
}
