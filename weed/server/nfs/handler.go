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

func (h *Handler) Mount(_ context.Context, conn net.Conn, req gonfs.MountRequest) (gonfs.MountStatus, billy.Filesystem, []gonfs.AuthFlavor) {
	if h.server.clientAuthorizer != nil && !h.server.clientAuthorizer.isAllowedConn(conn) {
		return gonfs.MountStatusErrAcces, nil, []gonfs.AuthFlavor{gonfs.AuthFlavorNull}
	}
	if requested := normalizeExportRoot(util.FullPath(req.Dirpath)); requested != h.server.exportRoot {
		glog.V(0).Infof("nfs mount: client requested %q; serving configured export %q", string(req.Dirpath), h.server.exportRoot)
	}
	if _, err := h.rootFS.Lstat("/"); err != nil {
		if os.IsNotExist(err) {
			return gonfs.MountStatusErrNoEnt, nil, []gonfs.AuthFlavor{gonfs.AuthFlavorNull}
		}
		return gonfs.MountStatusErrServerFault, nil, []gonfs.AuthFlavor{gonfs.AuthFlavorNull}
	}
	return gonfs.MountStatusOk, h.rootFS, []gonfs.AuthFlavor{gonfs.AuthFlavorNull, gonfs.AuthFlavorUnix}
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
