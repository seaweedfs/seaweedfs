package command

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/mount/winfsp"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
)

// ownedByMounter reports entries as belonging to whoever started the mount,
// matching the uid=-1 option handed to WinFsp.
const ownedByMounter = ^uint32(0)

func RunMount(option *MountOptions, umask os.FileMode) bool {
	chunkSizeLimitMB := *mountOptions.chunkSizeLimitMB
	if chunkSizeLimitMB <= 0 {
		fmt.Printf("Please specify a reasonable buffer size.\n")
		return false
	}

	dir := *option.dir
	if dir == "" {
		fmt.Printf("Please specify the mount point via \"-dir\", for example -dir=S:\n")
		return false
	}
	// A drive letter or a not-yet-existing directory is what WinFsp wants, so
	// the mount point deliberately goes through none of the unix preparation:
	// no ResolvePath, no auto-create, no stat.
	if err := checkWindowsMountPoint(dir); err != nil {
		fmt.Printf("%v\n", err)
		return false
	}

	filerAddresses, grpcDialOption, cipher, bucketRootPath, ok := connectToFiler(option)
	if !ok {
		return true
	}

	if *option.localSocket == "" {
		mountDirHash := util.HashToInt32([]byte(dir))
		if mountDirHash < 0 {
			mountDirHash = -mountDirHash
		}
		*option.localSocket = filepath.Join(os.TempDir(), fmt.Sprintf("seaweedfs-mount-%d.sock", mountDirHash))
	}
	if err := os.Remove(*option.localSocket); err != nil && !os.IsNotExist(err) {
		glog.Fatalf("Failed to remove %s, error: %s", *option.localSocket, err.Error())
	}
	mountSocketListener, err := net.Listen("unix", *option.localSocket)
	if err != nil {
		glog.Fatalf("Failed to listen on %s: %v", *option.localSocket, err)
	}

	uidGidMapper, err := meta_cache.NewUidGidMapper(*option.uidMap, *option.gidMap)
	if err != nil {
		fmt.Printf("failed to parse %s %s: %v\n", *option.uidMap, *option.gidMap, err)
		return false
	}

	mountRoot := resolveMountRoot(*option.filerMountRootPath)
	cacheDirForRead, cacheDirForWrite := resolveCacheDirs(option)

	seaweedFileSystem := buildSeaweedFileSystem(option, fileSystemParams{
		dir:              dir,
		mountRoot:        mountRoot,
		filerAddresses:   filerAddresses,
		grpcDialOption:   grpcDialOption,
		cipher:           cipher,
		uidGidMapper:     uidGidMapper,
		uid:              ownedByMounter,
		gid:              ownedByMounter,
		mountMode:        os.ModeDir | 0777,
		mountCtime:       time.Now(),
		umask:            umask,
		chunkSizeLimitMB: chunkSizeLimitMB,
		cacheDirForRead:  cacheDirForRead,
		cacheDirForWrite: cacheDirForWrite,
	})

	if !createMountRoot(seaweedFileSystem, mountRoot, bucketRootPath, filerAddresses) {
		return false
	}

	host := winfsp.New(seaweedFileSystem, winfsp.Options{
		VolumeName:      strings.ReplaceAll(*option.filer, ",", "+"),
		CaseInsensitive: option.windowsCaseInsensitive != nil && *option.windowsCaseInsensitive,
		Uid:             ownedByMounter,
		Gid:             ownedByMounter,
		AttrTimeout:     float64(*option.cacheMetaTtlSec),
		ReadOnly:        *option.readOnly,
		Debug:           *option.debugFuse,
		ExtraOptions:    option.extraOptions,
	})

	grace.OnInterrupt(func() {
		host.Unmount()
	})

	serveMountGrpc(seaweedFileSystem, mountSocketListener)

	if err := seaweedFileSystem.StartBackgroundTasks(); err != nil {
		fmt.Printf("failed to start background tasks: %v\n", err)
		return false
	}

	glog.V(0).Infof("mounting %s%s to %v", *option.filer, mountRoot, dir)
	glog.V(0).Infof("This is SeaweedFS version %s %s %s", version.Version(), runtime.GOOS, runtime.GOARCH)
	glog.V(0).Infof("Windows mount is beta: hard links are unavailable and byte-range locks are not shared across mounts")

	if err := host.Serve(dir); err != nil {
		glog.Errorf("%v", err)
		return false
	}

	seaweedFileSystem.WaitForAsyncFlush()
	seaweedFileSystem.ClearCacheDir()

	return true
}

// checkWindowsMountPoint rejects the mount points WinFsp cannot take, which is
// worth doing up front because its own failure is a bare false.
func checkWindowsMountPoint(dir string) error {
	if isDriveLetter(dir) {
		if _, err := os.Stat(dir + `\`); err == nil {
			return fmt.Errorf("drive %s is already in use", dir)
		}
		return nil
	}
	if strings.HasPrefix(dir, `\\`) {
		return nil
	}
	if _, err := os.Stat(dir); err == nil {
		return fmt.Errorf("mount point %s already exists; WinFsp needs a drive letter or a path that does not exist yet", dir)
	}
	if _, err := os.Stat(filepath.Dir(dir)); err != nil {
		return fmt.Errorf("parent of mount point %s does not exist", dir)
	}
	return nil
}

func isDriveLetter(dir string) bool {
	if len(dir) != 2 || dir[1] != ':' {
		return false
	}
	c := dir[0]
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
}
