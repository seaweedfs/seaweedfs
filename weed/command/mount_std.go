//go:build linux || darwin || freebsd

package command

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/user"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/seaweedfs/seaweedfs/weed/util/version"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mount"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/mount/unmount"

	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
)

func RunMount(option *MountOptions, umask os.FileMode) bool {
	if err := configureMountMemory(option); err != nil {
		fmt.Println(err)
		return false
	}

	// basic checks
	chunkSizeLimitMB := *mountOptions.chunkSizeLimitMB
	if chunkSizeLimitMB <= 0 {
		fmt.Printf("Please specify a reasonable buffer size.\n")
		return false
	}
	if err := checkIdmap(runtime.GOOS, *option.idmap, *option.defaultPermissions); err != nil {
		fmt.Println(err)
		return false
	}

	filerAddresses, grpcDialOption, cipher, bucketRootPath, ok := connectToFiler(option)
	if !ok {
		return true
	}

	filerMountRootPath := *option.filerMountRootPath
	var sseReader *mount.SSES3Reader
	if *option.sseS3Read {
		bucketPath, ok := bucketPathForMountRoot(filerMountRootPath, bucketRootPath)
		if !ok || !*option.readOnly || *option.allowOthers {
			glog.Errorf("-sseS3Read requires a bucket-root mount, -readOnly and -allowOthers=false")
			return false
		}
		if resolveMountRoot(filerMountRootPath) != bucketPath {
			glog.Errorf("-sseS3Read requires -filer.path=%s", bucketPath)
			return false
		}
		for _, opt := range option.extraOptions {
			for _, token := range strings.Split(opt, ",") {
				switch token {
				case "allow_other", "allow_root", "rw":
					glog.Errorf("-sseS3Read is incompatible with mount option %q", token)
					return false
				}
			}
		}
		var err error
		sseReader, err = mount.NewSSES3Reader(context.Background(), bucketPath, filerAddresses, grpcDialOption)
		if err != nil {
			glog.Errorf("configure SSE-S3 reads: %v", err)
			return false
		}
	}

	// clean up mount point
	dir := util.ResolvePath(*option.dir)
	if dir == "" {
		fmt.Printf("Please specify the mount directory via \"-dir\"")
		return false
	}

	if err := unmount.Unmount(dir); err != nil {
		glog.V(1).Infof("pre-mount cleanup unmount %s: %v", dir, err)
	}

	// start on local unix socket
	if *option.localSocket == "" {
		mountDirHash := util.HashToInt32([]byte(dir))
		if mountDirHash < 0 {
			mountDirHash = -mountDirHash
		}
		*option.localSocket = fmt.Sprintf("/tmp/seaweedfs-mount-%d.sock", mountDirHash)
	}
	if err := os.Remove(*option.localSocket); err != nil && !os.IsNotExist(err) {
		glog.Fatalf("Failed to remove %s, error: %s", *option.localSocket, err.Error())
	}
	montSocketListener, err := net.Listen("unix", *option.localSocket)
	if err != nil {
		glog.Fatalf("Failed to listen on %s: %v", *option.localSocket, err)
	}
	removeMountSocket := mountSocketCleanup(*option.localSocket)
	defer removeMountSocket()
	grace.OnInterrupt(removeMountSocket)

	// detect mount folder mode
	if *option.dirAutoCreate {
		if err := os.MkdirAll(dir, os.FileMode(0777)&^umask); err != nil {
			removeMountSocket()
			glog.Fatalf("failed to create directory %s:%v", dir, err)
		}
	}
	fileInfo, err := os.Stat(dir)

	// collect uid, gid
	uid, gid := uint32(0), uint32(0)
	mountMode := os.ModeDir | 0777
	if err == nil {
		mountMode = os.ModeDir | os.FileMode(0777)&^umask
		uid, gid = util.GetFileUidGid(fileInfo)
		fmt.Printf("mount point owner uid=%d gid=%d mode=%s\n", uid, gid, mountMode)
	} else {
		fmt.Printf("can not stat %s\n", dir)
		return false
	}

	// detect uid, gid
	if uid == 0 {
		if u, err := user.Current(); err == nil {
			if parsedId, pe := strconv.ParseUint(u.Uid, 10, 32); pe == nil {
				uid = uint32(parsedId)
			}
			if parsedId, pe := strconv.ParseUint(u.Gid, 10, 32); pe == nil {
				gid = uint32(parsedId)
			}
			fmt.Printf("current uid=%d gid=%d\n", uid, gid)
		}
	}

	// mapping uid, gid
	uidGidMapper, err := meta_cache.NewUidGidMapper(*option.uidMap, *option.gidMap)
	if err != nil {
		fmt.Printf("failed to parse %s %s: %v\n", *option.uidMap, *option.gidMap, err)
		return false
	}

	// Ensure target mount point availability
	skipAutofs := option.hasAutofs != nil && *option.hasAutofs
	if isValid := checkMountPointAvailable(dir, skipAutofs); !isValid {
		removeMountSocket()
		glog.Fatalf("Target mount point is not available: %s, please check!", dir)
		return true
	}

	serverFriendlyName := strings.ReplaceAll(*option.filer, ",", "+")

	// When autofs/systemd-mount is used, FsName must be "fuse" so util-linux/mount can recognize
	// it as a pseudo filesystem. Otherwise, preserve the descriptive name for mount/df output.
	fsName := serverFriendlyName + ":" + filerMountRootPath
	if *option.volumeName != "" {
		fsName = *option.volumeName
	}
	if skipAutofs {
		fsName = "fuse"
	}

	maxBackground := 128
	if option.fuseMaxBackground != nil && *option.fuseMaxBackground > 0 {
		maxBackground = *option.fuseMaxBackground
	}
	congestionThreshold := 0
	if option.fuseCongestionThreshold != nil && *option.fuseCongestionThreshold > 0 {
		congestionThreshold = *option.fuseCongestionThreshold
	}

	// mount fuse
	fuseMountOptions := &fuse.MountOptions{
		AllowOther:               *option.allowOthers,
		MaxBackground:            maxBackground,
		CongestionThreshold:      congestionThreshold,
		MaxWrite:                 1024 * 1024 * 2,
		MaxReadAhead:             1024 * 1024 * 2,
		IgnoreSecurityLabels:     false,
		RememberInodes:           false,
		FsName:                   fsName,
		Name:                     "seaweedfs",
		SingleThreaded:           false,
		DisableXAttrs:            *option.disableXAttr,
		Debug:                    *option.debugFuse,
		EnableLocks:              true,
		ExplicitDataCacheControl: false,
		DirectMount:              true,
		DirectMountFlags:         0,
		IDMappedMount:            *option.idmap,
		//SyncRead:                 false, // set to false to enable the FUSE_CAP_ASYNC_READ capability
		EnableAcl: true,
	}
	if *option.defaultPermissions {
		fuseMountOptions.Options = append(fuseMountOptions.Options, "default_permissions")
	}
	if *option.nonempty {
		fuseMountOptions.Options = append(fuseMountOptions.Options, "nonempty")
	}
	if *option.readOnly {
		if runtime.GOOS == "darwin" {
			fuseMountOptions.Options = append(fuseMountOptions.Options, "rdonly")
		} else {
			fuseMountOptions.Options = append(fuseMountOptions.Options, "ro")
		}
	}
	if runtime.GOOS == "darwin" {
		// https://github-wiki-see.page/m/macfuse/macfuse/wiki/Mount-Options
		ioSizeMB := 1
		for ioSizeMB*2 <= *option.chunkSizeLimitMB && ioSizeMB*2 <= 32 {
			ioSizeMB *= 2
		}
		fuseMountOptions.Options = append(fuseMountOptions.Options, "daemon_timeout=600")
		if runtime.GOARCH == "amd64" {
			fuseMountOptions.Options = append(fuseMountOptions.Options, "noapplexattr")
		}
		if option.novncache != nil && *option.novncache {
			fuseMountOptions.Options = append(fuseMountOptions.Options, "novncache")
		}
		fuseMountOptions.Options = append(fuseMountOptions.Options, "slow_statfs")
		fuseMountOptions.Options = append(fuseMountOptions.Options, "volname="+volumeName(*option.filer, filerMountRootPath, dir, *option.volumeName))
		fuseMountOptions.Options = append(fuseMountOptions.Options, fmt.Sprintf("iosize=%d", ioSizeMB*1024*1024))
	}
	// Last, so an option given on the command line wins over the default
	// this mount picked for it.
	fuseMountOptions.Options = append(fuseMountOptions.Options, option.extraOptions...)

	if option.writebackCache != nil {
		fuseMountOptions.EnableWriteback = *option.writebackCache
	}
	if option.asyncDio != nil {
		fuseMountOptions.EnableAsyncDio = *option.asyncDio
	}
	if option.nfsExport != nil {
		fuseMountOptions.EnableExportSupport = *option.nfsExport
	}
	if option.cacheSymlink != nil && *option.cacheSymlink {
		fuseMountOptions.EnableSymlinkCaching = true
	}

	mountRoot := resolveMountRoot(filerMountRootPath)
	cacheDirForRead, cacheDirForWrite := resolveCacheDirs(option)

	seaweedFileSystem := buildSeaweedFileSystem(option, fileSystemParams{
		sseS3Reader:      sseReader,
		dir:              dir,
		mountRoot:        mountRoot,
		filerAddresses:   filerAddresses,
		grpcDialOption:   grpcDialOption,
		cipher:           cipher,
		uidGidMapper:     uidGidMapper,
		uid:              uid,
		gid:              gid,
		mountMode:        mountMode,
		mountCtime:       fileInfo.ModTime(),
		umask:            umask,
		chunkSizeLimitMB: chunkSizeLimitMB,
		cacheDirForRead:  cacheDirForRead,
		cacheDirForWrite: cacheDirForWrite,
	})

	if !createMountRoot(seaweedFileSystem, mountRoot, bucketRootPath, filerAddresses) {
		return false
	}

	server, err := fuse.NewServer(seaweedFileSystem, dir, fuseMountOptions)
	if err != nil {
		// A failed mount is an environment problem (no /dev/fuse, fusermount not
		// setuid, stale mount point); the goroutine dump Fatalf adds buries it.
		removeMountSocket()
		glog.Exitf("Mount fail: %v", err)
	}
	if *option.idmap && server.KernelSettings().Flags64()&fuse.CAP_ALLOW_IDMAP == 0 {
		glog.Warningf("-idmap: this kernel does not support ID-mapped FUSE mounts (Linux 6.12 or later), so ID-mapped bind mounts of %s will fail", dir)
	}
	grace.OnInterrupt(func() {
		if err := unmount.Unmount(dir); err != nil {
			glog.Errorf("failed to unmount %s: %v", dir, err)
		}
	})

	if mountOptions.fuseCommandPid != 0 {
		// send a signal to the parent process to notify that the mount is ready
		err = syscall.Kill(mountOptions.fuseCommandPid, syscall.SIGTERM)
		if err != nil {
			fmt.Printf("failed to notify parent process: %v\n", err)
			return false
		}
	}

	serveMountGrpc(seaweedFileSystem, montSocketListener)

	err = seaweedFileSystem.StartBackgroundTasks()
	if err != nil {
		fmt.Printf("failed to start background tasks: %v\n", err)
		return false
	}

	glog.V(0).Infof("mounted %s%s to %v", *option.filer, mountRoot, dir)
	glog.V(0).Infof("This is SeaweedFS version %s %s %s", version.Version(), runtime.GOOS, runtime.GOARCH)

	server.Serve()

	// Wait for any pending background flushes (writebackCache async mode)
	// before clearing caches, to prevent data loss during clean unmount.
	seaweedFileSystem.WaitForAsyncFlush()

	seaweedFileSystem.ClearCacheDir()

	return true
}

// checkIdmap refuses -idmap on an OS other than Linux and with
// -defaultPermissions=false. The kernel accepts FUSE_ALLOW_IDMAP only with
// default_permissions, and go-fuse then adds that option on its own, so the
// kernel would ignore -defaultPermissions=false.
func checkIdmap(goos string, idmap, defaultPermissions bool) error {
	switch {
	case !idmap:
		return nil
	case goos != "linux":
		return fmt.Errorf("-idmap is only supported on Linux")
	case !defaultPermissions:
		return fmt.Errorf("-idmap requires -defaultPermissions")
	}
	return nil
}
