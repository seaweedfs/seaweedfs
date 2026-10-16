package weed_server

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/constants"
)

type FilerExtractResult struct {
	Name      string             `json:"name,omitempty"`
	Extracted []*FilerPostResult `json:"extracted,omitempty"`
	Errors    []string           `json:"errors,omitempty"`
}

func isExtractZip(r *http.Request) bool {
	return r.URL.Query().Get("extract") == "zip" ||
		r.Header.Get(s3_constants.SeaweedFSExtract) == "zip"
}

// extractZipArchive writes a posted archive's members as individual entries
// under the target directory, like AWS Snowball's auto-extract. The archive
// itself is only stored when ?extract.keep=true.
func (fs *FilerServer) extractZipArchive(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var reader io.Reader = r.Body
	archiveName := path.Base(r.URL.Path)
	if r.Method == http.MethodPost {
		multipartReader, multipartReaderErr := r.MultipartReader()
		if multipartReaderErr != nil {
			writeJsonError(w, r, http.StatusBadRequest, multipartReaderErr)
			return
		}
		part1, part1Err := multipartReader.NextPart()
		if part1Err != nil {
			writeJsonError(w, r, http.StatusBadRequest, part1Err)
			return
		}
		defer part1.Close()
		reader = part1
		if fileName := part1.FileName(); fileName != "" {
			archiveName = path.Base(fileName)
		}
	}

	ra, size, cleanup, err := filer.SpoolZipReader(reader)
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, fmt.Errorf("read input: %w", err))
		return
	}
	defer cleanup()
	zr, err := zip.NewReader(ra, size)
	if err != nil {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("extract zip: %w", err))
		return
	}
	if err = filer.CheckZipLimits(zr); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}

	root := r.URL.Path
	if !strings.HasSuffix(root, "/") {
		root = path.Dir(root)
	}
	root = strings.TrimSuffix(root, "/")

	query := r.URL.Query()
	parsedMaxMB, _ := strconv.ParseInt(query.Get("maxMB"), 10, 32)
	maxMB := int32(parsedMaxMB)
	if maxMB <= 0 && fs.option.MaxMB > 0 {
		maxMB = int32(fs.option.MaxMB)
	}
	chunkSize := 1024 * 1024 * maxMB

	// Members may land under storage rules different from the request path;
	// resolve options per destination, keeping the request's explicit overrides.
	memberStorageOption := func(destPath string) (*operation.StorageOption, error) {
		memberSO, detectErr := fs.detectStorageOption0(ctx, destPath,
			query.Get("collection"),
			query.Get("replication"),
			query.Get("ttl"),
			query.Get("disk"),
			query.Get("fsync"),
			query.Get("dataCenter"),
			query.Get("rack"),
			query.Get("dataNode"),
			query.Get("saveInside"),
		)
		if detectErr != nil {
			return nil, detectErr
		}
		if memberSO.DiskType == "" {
			memberSO.DiskType = fs.option.DiskType
		}
		if strings.HasPrefix(destPath, "/etc") {
			memberSO.SaveInside = true
		}
		return memberSO, nil
	}

	result := &FilerExtractResult{Name: archiveName}
	writeMember := func(rel string, memberReader io.Reader, mtime time.Time, mode os.FileMode) {
		destPath := root + "/" + rel
		memberSO, soErr := memberStorageOption(destPath)
		if soErr != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", rel, soErr))
			return
		}
		memberResult, memberErr := fs.saveExtractedEntry(ctx, destPath, memberReader, path.Base(rel), chunkSize, memberSO, mtime, mode)
		if memberErr != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", rel, memberErr))
			return
		}
		memberResult.Name = rel
		result.Extracted = append(result.Extracted, memberResult)
	}
	mkdirMember := func(rel string, zf *zip.File) {
		destPath := root + "/" + rel
		memberSO, soErr := memberStorageOption(destPath)
		if soErr != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", rel, soErr))
			return
		}
		if dirErr := fs.mkdirExtracted(ctx, destPath, zf, memberSO); dirErr != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", rel, dirErr))
		}
	}

	if query.Get("extract.keep") == "true" && archiveName != "." && archiveName != "/" {
		mode := os.FileMode(0660)
		if modeStr := query.Get("mode"); modeStr != "" {
			if parsed, parseErr := strconv.ParseUint(modeStr, 8, 32); parseErr == nil {
				mode = os.FileMode(parsed)
			}
		}
		writeMember(archiveName, io.NewSectionReader(ra, 0, size), time.Now(), mode)
	}

	for _, zf := range zr.File {
		rel, ok := filer.SanitizeZipMemberName(zf)
		if !ok {
			result.Errors = append(result.Errors, fmt.Sprintf("%s: unsafe member name", zf.Name))
			continue
		}
		if zf.FileInfo().IsDir() {
			mkdirMember(rel, zf)
			continue
		}
		memberReader, openErr := zf.Open()
		if openErr != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", rel, openErr))
			continue
		}
		mode := os.FileMode(0660)
		if perm := zf.Mode().Perm(); perm != 0 {
			mode = perm
		}
		writeMember(rel, memberReader, zf.Modified, mode)
		memberReader.Close()
	}

	writeJsonQuiet(w, r, http.StatusCreated, result)
}

func (fs *FilerServer) mkdirExtracted(ctx context.Context, destPath string, zf *zip.File, so *operation.StorageOption) error {
	existing, findErr := fs.filer.FindEntry(ctx, util.FullPath(destPath))
	if findErr == nil {
		if existing.IsDirectory() {
			return nil
		}
		return fmt.Errorf("%s already exists as a file", destPath)
	}
	if !errors.Is(findErr, filer_pb.ErrNotFound) {
		return findErr
	}
	mode := os.FileMode(0660)
	if perm := zf.Mode().Perm(); perm != 0 {
		mode = perm
	}
	mtime := zf.Modified
	if mtime.IsZero() {
		mtime = time.Now()
	}
	return fs.filer.CreateEntry(context.WithoutCancel(ctx), &filer.Entry{
		FullPath: util.FullPath(destPath),
		Attr: filer.Attr{
			Mtime:  mtime,
			Crtime: time.Now(),
			Mode:   mode | os.ModeDir,
			Uid:    OS_UID,
			Gid:    OS_GID,
			TtlSec: so.TtlSeconds,
		},
	}, nil, false, false, nil, false, so.MaxFileNameLength)
}

func (fs *FilerServer) saveExtractedEntry(ctx context.Context, destPath string, reader io.Reader, fileName string, chunkSize int32, so *operation.StorageOption, mtime time.Time, mode os.FileMode) (*FilerPostResult, error) {
	if fs.filer.FilerConf.MatchStorageRule(destPath).ReadOnly {
		return nil, ErrReadOnly
	}
	wormEnforced, err := fs.wormEnforcedForEntry(ctx, destPath)
	if err != nil {
		return nil, err
	}
	if wormEnforced {
		return nil, errors.New(constants.ErrMsgOperationNotPermitted)
	}
	if util.FullPath(destPath).IsLongerFileName(so.MaxFileNameLength) {
		return nil, filer_pb.ErrEntryNameTooLong
	}

	contentType := mime.TypeByExtension(path.Ext(fileName))
	fileChunks, md5Hash, chunkOffset, uploadErr, smallContent := fs.uploadReaderToChunks(ctx, nil, reader, 0, chunkSize, fileName, contentType, false, so)
	if uploadErr != nil {
		return nil, uploadErr
	}
	if chunkOffset == 0 {
		smallContent = []byte{}
	}

	mergedChunks, mergeErr := fs.maybeMergeChunks(ctx, so, fileChunks)
	if mergeErr != nil {
		glog.V(0).InfofCtx(ctx, "merge chunks %s: %v", destPath, mergeErr)
		mergedChunks = fileChunks
	}
	mergedChunks, manifestErr := filer.MaybeManifestize(fs.saveAsChunk(ctx, so), fs.filer.DeleteChunksNotRecursive, mergedChunks)
	if manifestErr != nil {
		fs.filer.DeleteUncommittedChunks(ctx, mergedChunks)
		return nil, manifestErr
	}

	if mtime.IsZero() {
		mtime = time.Now()
	}
	entry := &filer.Entry{
		FullPath: util.FullPath(destPath),
		Attr: filer.Attr{
			Mtime:    mtime,
			Crtime:   time.Now(),
			Mode:     mode,
			Uid:      OS_UID,
			Gid:      OS_GID,
			TtlSec:   so.TtlSeconds,
			Mime:     contentType,
			Md5:      md5Hash.Sum(nil),
			FileSize: uint64(chunkOffset),
		},
		Content: smallContent,
		Chunks:  mergedChunks,
	}
	if err := fs.filer.CreateEntry(context.WithoutCancel(ctx), entry, nil, false, false, nil, false, so.MaxFileNameLength); err != nil {
		fs.filer.DeleteUncommittedChunks(ctx, mergedChunks)
		return nil, err
	}
	return &FilerPostResult{Name: fileName, Size: chunkOffset}, nil
}
