//go:build linux || darwin || freebsd || windows

package command

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mount"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mount_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
)

func runMount(cmd *Command, args []string) bool {

	if *mountOptions.debug {
		go http.ListenAndServe(fmt.Sprintf(":%d", *mountOptions.debugPort), nil)
	}

	*mountCpuProfile = util.ResolvePath(*mountCpuProfile)
	*mountMemProfile = util.ResolvePath(*mountMemProfile)
	grace.SetupProfiling(*mountCpuProfile, *mountMemProfile)
	if *mountReadRetryTime < time.Second {
		*mountReadRetryTime = time.Second
	}
	util.RetryWaitTime = *mountReadRetryTime

	// 32 bits, not 64: os.FileMode is uint32, so a wider parse would let a
	// nonsense umask truncate silently instead of being rejected here.
	umask, umaskErr := strconv.ParseUint(*mountOptions.umaskString, 8, 32)
	if umaskErr != nil {
		fmt.Printf("can not parse umask %s", *mountOptions.umaskString)
		return false
	}

	if len(args) > 0 {
		return false
	}

	return RunMount(&mountOptions, os.FileMode(umask))
}

func ensureBucketAllowEmptyFolders(ctx context.Context, filerClient filer_pb.FilerClient, mountRoot, bucketRootPath string) error {
	bucketPath, isBucketRootMount := bucketPathForMountRoot(mountRoot, bucketRootPath)
	if !isBucketRootMount {
		return nil
	}

	entry, _, _, err := filer_pb.GetEntry(ctx, filerClient, util.FullPath(bucketPath))
	if err != nil {
		return err
	}
	if entry == nil {
		return fmt.Errorf("bucket %s not found", bucketPath)
	}

	if entry.Extended == nil {
		entry.Extended = make(map[string][]byte)
	}
	if strings.EqualFold(strings.TrimSpace(string(entry.Extended[s3_constants.ExtAllowEmptyFolders])), "true") {
		return nil
	}

	entry.Extended[s3_constants.ExtAllowEmptyFolders] = []byte("true")

	bucketFullPath := util.FullPath(bucketPath)
	parent, _ := bucketFullPath.DirAndName()
	if err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer_pb.UpdateEntry(ctx, client, &filer_pb.UpdateEntryRequest{
			Directory: parent,
			Entry:     entry,
		})
	}); err != nil {
		return err
	}

	glog.V(3).Infof("RunMount: set bucket %s %s=true", bucketPath, s3_constants.ExtAllowEmptyFolders)
	return nil
}

func bucketPathForMountRoot(mountRoot, bucketRootPath string) (string, bool) {
	cleanPath := path.Clean("/" + strings.TrimPrefix(mountRoot, "/"))
	cleanBucketRoot := path.Clean("/" + strings.TrimPrefix(bucketRootPath, "/"))
	if cleanBucketRoot == "/" {
		return "", false
	}
	prefix := cleanBucketRoot + "/"
	if !strings.HasPrefix(cleanPath, prefix) {
		return "", false
	}
	rest := strings.TrimPrefix(cleanPath, prefix)

	bucketParts := strings.Split(rest, "/")
	if len(bucketParts) != 1 || bucketParts[0] == "" {
		return "", false
	}
	return cleanBucketRoot + "/" + bucketParts[0], true
}

func peerStringOrEmpty(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// connectToFiler retries the filer handshake, returning the cluster's cipher
// setting and bucket root.
func connectToFiler(option *MountOptions) (filerAddresses []pb.ServerAddress, grpcDialOption grpc.DialOption, cipher bool, bucketRootPath string, ok bool) {
	// try to connect to filer
	filerAddresses = pb.ServerAddresses(*option.filer).ToAddresses()
	util.LoadSecurityConfiguration()
	grpcDialOption = security.LoadClientTLS(util.GetViper(), "grpc.client")
	var err error
	for i := 0; i < 10; i++ {
		err = pb.WithOneOfGrpcFilerClients(false, filerAddresses, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
			if err != nil {
				return fmt.Errorf("get filer grpc address %v configuration: %w", filerAddresses, err)
			}
			cipher = resp.Cipher
			bucketRootPath = resp.DirBuckets
			return nil
		})
		if err == nil {
			break
		}
		glog.V(0).Infof("failed to talk to filer %v: %v", filerAddresses, err)
		glog.V(0).Infof("wait for %d seconds ...", i+1)
		time.Sleep(time.Duration(i+1) * time.Second)
	}
	if err != nil {
		glog.Errorf("failed to talk to filer %v: %v", filerAddresses, err)
		return nil, nil, false, "", false
	}
	if bucketRootPath == "" {
		bucketRootPath = "/buckets"
	}
	return filerAddresses, grpcDialOption, cipher, bucketRootPath, true
}

// fileSystemParams are the pieces of a mount that each platform works out for
// itself: where it is attached, and whose identity the entries carry.
type fileSystemParams struct {
	dir              string
	mountRoot        string
	filerAddresses   []pb.ServerAddress
	grpcDialOption   grpc.DialOption
	cipher           bool
	uidGidMapper     *meta_cache.UidGidMapper
	uid              uint32
	gid              uint32
	mountMode        os.FileMode
	mountCtime       time.Time
	umask            os.FileMode
	chunkSizeLimitMB int
	cacheDirForRead  string
	cacheDirForWrite string

	// eagerFilerCreate persists a created file's entry at create time rather
	// than at flush. A platform sets it when it cannot run the flush before
	// the application's close returns, so nothing that reads through the
	// filer can race an unflushed close.
	eagerFilerCreate bool
}

func buildSeaweedFileSystem(option *MountOptions, p fileSystemParams) *mount.WFS {
	return mount.NewSeaweedFileSystem(&mount.Option{
		MountDirectory:              p.dir,
		FilerAddresses:              p.filerAddresses,
		GrpcDialOption:              p.grpcDialOption,
		FilerSigningKey:             security.SigningKey(util.GetViper().GetString("jwt.filer_signing.key")),
		FilerSigningExpiresAfterSec: util.GetViper().GetInt("jwt.filer_signing.expires_after_seconds"),
		FilerMountRootPath:          p.mountRoot,
		Collection:                  *option.collection,
		Replication:                 *option.replication,
		TtlSec:                      int32(*option.ttlSec),
		DiskType:                    types.ToDiskType(*option.diskType),
		ChunkSizeLimit:              int64(p.chunkSizeLimitMB) * 1024 * 1024,
		ConcurrentWriters:           *option.concurrentWriters,
		ConcurrentReaders:           *option.concurrentReaders,
		CacheDirForRead:             p.cacheDirForRead,
		CacheSizeMBForRead:          *option.cacheSizeMBForRead,
		CacheDirForWrite:            p.cacheDirForWrite,
		WriteBufferSizeMB:           *option.writeBufferSizeMB,
		CacheMetaTTlSec:             *option.cacheMetaTtlSec,
		CacheDirMaxEntries:          *option.cacheDirMaxEntries,
		MaxInodeEntries:             *option.maxInodeEntries,
		DataCenter:                  *option.dataCenter,
		Quota:                       int64(*option.collectionQuota) * 1024 * 1024,
		LogicalDiskUsage:            *option.logicalDiskUsage,
		MountUid:                    p.uid,
		MountGid:                    p.gid,
		MountMode:                   p.mountMode,
		MountCtime:                  p.mountCtime,
		MountMtime:                  time.Now(),
		Umask:                       p.umask,
		VolumeServerAccess:          *mountOptions.volumeServerAccess,
		Cipher:                      p.cipher,
		UidGidMapper:                p.uidGidMapper,
		IncludeSystemEntries:        *option.includeSystemEntries,
		DefaultPermissions:          *option.defaultPermissions,
		DisableXAttr:                *option.disableXAttr,
		IsMacOs:                     runtime.GOOS == "darwin",
		MetadataFlushSeconds:        *option.metadataFlushSeconds,
		// RDMA acceleration options
		RdmaEnabled:           *option.rdmaEnabled,
		RdmaSidecarAddr:       *option.rdmaSidecarAddr,
		RdmaFallback:          *option.rdmaFallback,
		RdmaReadOnly:          *option.rdmaReadOnly,
		RdmaMaxConcurrent:     *option.rdmaMaxConcurrent,
		RdmaTimeoutMs:         *option.rdmaTimeoutMs,
		DirIdleEvictSec:       *option.dirIdleEvictSec,
		EnableDistributedLock: option.distributedLock != nil && *option.distributedLock,
		WritebackCache:        option.writebackCache != nil && *option.writebackCache,
		EagerFilerCreate:      p.eagerFilerCreate,
		PosixDirNlink:         option.posixDirNlink != nil && *option.posixDirNlink,
		// Peer chunk sharing
		PeerEnabled:    option.peerEnabled != nil && *option.peerEnabled,
		PeerListen:     peerStringOrEmpty(option.peerListen),
		PeerAdvertise:  peerStringOrEmpty(option.peerAdvertise),
		PeerDataCenter: peerStringOrEmpty(option.peerDataCenter),
		PeerRack:       peerStringOrEmpty(option.peerRack),
	})
}

// createMountRoot makes the filer-side directory the mount is rooted at.
func createMountRoot(wfs *mount.WFS, mountRoot, bucketRootPath string, filerAddresses []pb.ServerAddress) bool {
	mountRootPath := util.FullPath(mountRoot)
	mountRootParent, mountDir := mountRootPath.DirAndName()
	if err := filer_pb.Mkdir(context.Background(), wfs, mountRootParent, mountDir, nil); err != nil {
		fmt.Printf("failed to create dir %s on filer %s: %v\n", mountRoot, filerAddresses, err)
		return false
	}
	if err := ensureBucketAllowEmptyFolders(context.Background(), wfs, mountRoot, bucketRootPath); err != nil {
		fmt.Printf("failed to set bucket auto-remove-empty-folders policy for %s: %v\n", mountRoot, err)
		return false
	}
	return true
}

// serveMountGrpc exposes the local control socket used by "weed mount.stats".
func serveMountGrpc(wfs *mount.WFS, listener net.Listener) {
	grpcS := pb.NewGrpcServer()
	mount_pb.RegisterSeaweedMountServer(grpcS, wfs)
	reflection.Register(grpcS)
	go grpcS.Serve(listener)
}

// resolveMountRoot trims the trailing slash the filer path must not carry.
func resolveMountRoot(filerMountRootPath string) string {
	mountRoot := filerMountRootPath
	if mountRoot != "/" && strings.HasSuffix(mountRoot, "/") {
		mountRoot = mountRoot[0 : len(mountRoot)-1]
	}
	return mountRoot
}

// resolveCacheDirs falls back to the read cache when no write cache is set.
func resolveCacheDirs(option *MountOptions) (string, string) {
	cacheDirForRead := util.ResolvePath(*option.cacheDirForRead)
	cacheDirForWrite := util.ResolvePath(*option.cacheDirForWrite)
	if cacheDirForWrite == "" {
		cacheDirForWrite = cacheDirForRead
	}
	return cacheDirForRead, cacheDirForWrite
}
