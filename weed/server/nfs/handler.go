package nfs

import (
	"context"
	"net"
	"os"
	"strings"

	billy "github.com/go-git/go-billy/v5"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	gonfs "github.com/willscott/go-nfs"
)

type Handler struct {
	server *Server
	rootFS *seaweedFileSystem
}

var _ gonfs.Handler = (*Handler)(nil)

func (h *Handler) Mount(ctx context.Context, conn net.Conn, req gonfs.MountRequest) (gonfs.MountStatus, billy.Filesystem, []gonfs.AuthFlavor) {
	if h.server.clientAuthorizer != nil && !h.server.clientAuthorizer.isAllowedConn(conn) {
		return gonfs.MountStatusErrAcces, nil, []gonfs.AuthFlavor{gonfs.AuthFlavorNull}
	}
	fs, status := h.resolveMountFilesystem(ctx, req.Dirpath)
	if status != gonfs.MountStatusOk {
		return status, nil, []gonfs.AuthFlavor{gonfs.AuthFlavorNull}
	}
	return gonfs.MountStatusOk, fs, []gonfs.AuthFlavor{gonfs.AuthFlavorNull, gonfs.AuthFlavorUnix}
}

// resolveMountFilesystem resolves the MOUNT3 dirpath to a filesystem:
// exact match serves the export root; a path strictly under the export
// is mounted at that subdirectory (NoEnt/NotDir if missing or not a
// directory); anything else falls back to the export root with an INFO
// log. The UDP MOUNT path mirrors this in mount_udp.go.
func (h *Handler) resolveMountFilesystem(ctx context.Context, rawDirpath []byte) (*seaweedFileSystem, gonfs.MountStatus) {
	requested := normalizeExportRoot(util.FullPath(rawDirpath))
	if requested == h.server.exportRoot {
		return h.rootFS, h.lstatExportStatus(ctx)
	}
	if !requested.IsUnder(h.server.exportRoot) {
		glog.V(0).Infof("nfs mount: client requested %q (outside export %q); serving configured export", string(rawDirpath), h.server.exportRoot)
		return h.rootFS, h.lstatExportStatus(ctx)
	}
	entry, err := h.lookupSubexportEntry(ctx, requested)
	switch {
	case err != nil && isLookupNotFound(err):
		return nil, gonfs.MountStatusErrNoEnt
	case err != nil:
		glog.Errorf("nfs mount: lookup %q under export %q failed: %v", requested, h.server.exportRoot, err)
		return nil, gonfs.MountStatusErrServerFault
	case entry == nil:
		return nil, gonfs.MountStatusErrNoEnt
	case !entry.IsDirectory:
		return nil, gonfs.MountStatusErrNotDir
	}
	glog.V(1).Infof("nfs mount: client requested %q under export %q; mounting at subdirectory", string(rawDirpath), h.server.exportRoot)
	return newSeaweedFileSystem(h.server, requested, h.server.sharedReaderCache), gonfs.MountStatusOk
}

func (h *Handler) lstatExportStatus(ctx context.Context) gonfs.MountStatus {
	if _, err := h.rootFS.fileInfoForVirtualPath(ctx, "/"); err != nil {
		if os.IsNotExist(err) {
			return gonfs.MountStatusErrNoEnt
		}
		return gonfs.MountStatusErrServerFault
	}
	return gonfs.MountStatusOk
}

func (h *Handler) lookupSubexportEntry(ctx context.Context, p util.FullPath) (*filer_pb.Entry, error) {
	var entry *filer_pb.Entry
	err := h.server.withInternalClient(false, func(client nfsFilerClient) error {
		dir, name := p.DirAndName()
		resp, lerr := client.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		})
		if lerr != nil {
			return lerr
		}
		if resp != nil {
			entry = resp.Entry
		}
		return nil
	})
	return entry, err
}

func (h *Handler) Change(filesystem billy.Filesystem) billy.Change {
	if h.server != nil && h.server.option != nil && h.server.option.ReadOnly {
		return nil
	}
	if changer, ok := filesystem.(billy.Change); ok {
		return changer
	}
	return nil
}

func (h *Handler) FSStat(ctx context.Context, _ billy.Filesystem, stat *gonfs.FSStat) error {
	return h.server.withInternalClient(false, func(client nfsFilerClient) error {
		resp, err := client.Statistics(ctx, &filer_pb.StatisticsRequest{})
		if err != nil {
			return err
		}
		if resp == nil {
			return nil
		}
		stat.TotalSize = resp.TotalSize
		if resp.TotalSize >= resp.UsedSize {
			stat.FreeSize = resp.TotalSize - resp.UsedSize
			stat.AvailableSize = resp.TotalSize - resp.UsedSize
		}
		stat.TotalFiles = resp.FileCount
		return nil
	})
}

func (h *Handler) ToHandle(filesystem billy.Filesystem, path []string) []byte {
	fs, ok := filesystem.(*seaweedFileSystem)
	if !ok {
		fs = h.rootFS
	}

	info, err := fs.fileInfoForVirtualPath(context.Background(), fs.Join(path...))
	if err != nil {
		return nil
	}

	inode := info.entry.GetAttributes().GetInode()
	if inode == 0 && info.actualPath == h.server.exportRoot && info.entry.IsDirectory {
		return NewFileHandle(h.server.exportID, FileHandleKindDirectory, 0, filer.InodeIndexInitialGeneration).Encode()
	}

	return NewFileHandle(h.server.exportID, fileHandleKindForEntry(info.entry), inode, info.generation).Encode()
}

func (h *Handler) FromHandle(raw []byte) (billy.Filesystem, []string, error) {
	var resolved *ResolvedHandle
	err := h.server.withInternalClient(false, func(client nfsFilerClient) error {
		var resolveErr error
		resolved, resolveErr = NewResolver(h.server.exportRoot, client).ResolveHandle(context.Background(), raw)
		return resolveErr
	})
	if err != nil {
		return nil, nil, err
	}

	if resolved.Path == h.server.exportRoot {
		return h.rootFS, nil, nil
	}

	if !pathVisibleFromExport(resolved.Path, h.server.exportRoot) {
		return nil, nil, ErrHandleExportMismatch
	}

	relativePath := string(resolved.Path)
	if h.server.exportRoot != "/" {
		relativePath = strings.TrimPrefix(relativePath, string(h.server.exportRoot))
	}
	return h.rootFS, util.NormalizePath(relativePath).Split(), nil
}

func (h *Handler) InvalidateHandle(billy.Filesystem, []byte) error {
	return nil
}

func (h *Handler) HandleLimit() int {
	return h.server.handleLimit
}

func fileHandleKindForEntry(entry *filer_pb.Entry) FileHandleKind {
	if entry != nil && entry.IsDirectory {
		return FileHandleKindDirectory
	}
	return FileHandleKindFile
}
