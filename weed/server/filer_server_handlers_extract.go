package weed_server

import (
	"archive/zip"
	"bytes"
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

// extractZipRoot is the directory members land under: the request path when it
// ends in "/", otherwise its parent, so the path can name the archive.
func extractZipRoot(r *http.Request) string {
	root := r.URL.Path
	if !strings.HasSuffix(root, "/") {
		root = path.Dir(root)
	}
	return strings.TrimSuffix(root, "/")
}

// extractZipArchive writes a posted archive's members as individual entries
// under the target directory, like AWS Snowball's auto-extract. The archive
// itself is only stored when ?extract.keep=true.
func (fs *FilerServer) extractZipArchive(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var reader io.Reader = r.Body
	archiveName := path.Base(r.URL.Path)
	if r.Method == http.MethodPost {
		if mediaType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); strings.HasPrefix(mediaType, "multipart/") {
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
	}

	ra, size, cleanup, err := filer.SpoolZipReader(reader, fs.option.TusMaxSize)
	if err != nil {
		code := http.StatusInternalServerError
		if errors.Is(err, filer.ErrZipTooLarge) {
			code = http.StatusRequestEntityTooLarge
		}
		writeJsonError(w, r, code, fmt.Errorf("read input: %w", err))
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

	root := extractZipRoot(r)

	query := r.URL.Query()
	chunkSize := fs.requestChunkSize(query)

	// x-seaweedfs-destination picks storage rules for a logical destination
	// while members land under the request path, mirroring PostHandler. The
	// actual write path keeps its own read-only enforcement in
	// saveExtractedEntry and mkdirExtracted.
	ruleRoot := root
	if headerDestination := r.Header.Get(s3_constants.SeaweedStorageDestinationHeader); headerDestination != "" {
		ruleRoot = strings.TrimSuffix(headerDestination, "/")
	}

	// Members may land under storage rules different from the request path;
	// resolve options per destination, keeping the request's explicit overrides.
	memberStorageOption := func(rulePath, writePath string) (*operation.StorageOption, error) {
		memberSO, detectErr := fs.detectStorageOption0(ctx, rulePath,
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
		if strings.HasPrefix(writePath, "/etc") {
			memberSO.SaveInside = true
		}
		return memberSO, nil
	}

	result := &FilerExtractResult{Name: archiveName}
	writeMember := func(rel string, memberReader io.Reader, memberSize int64, mtime time.Time, mode os.FileMode) {
		destPath := root + "/" + rel
		memberSO, soErr := memberStorageOption(ruleRoot+"/"+rel, destPath)
		if soErr != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", rel, soErr))
			return
		}
		memberResult, memberErr := fs.saveExtractedEntry(ctx, destPath, memberReader, memberSize, path.Base(rel), chunkSize, memberSO, mtime, mode)
		if memberErr != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", rel, memberErr))
			return
		}
		memberResult.Name = rel
		result.Extracted = append(result.Extracted, memberResult)
	}
	mkdirMember := func(rel string, zf *zip.File) {
		destPath := root + "/" + rel
		memberSO, soErr := memberStorageOption(ruleRoot+"/"+rel, destPath)
		if soErr != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", rel, soErr))
			return
		}
		if dirErr := fs.mkdirExtracted(ctx, destPath, zf, memberSO); dirErr != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", rel, dirErr))
		}
	}

	if query.Get("extract.keep") == "true" && archiveName != "." && archiveName != ".." && archiveName != "/" {
		mode := os.FileMode(0660)
		if modeStr := query.Get("mode"); modeStr != "" {
			if parsed, parseErr := strconv.ParseUint(modeStr, 8, 32); parseErr == nil {
				mode = os.FileMode(parsed)
			}
		}
		writeMember(archiveName, io.NewSectionReader(ra, 0, size), size, time.Now(), mode)
	}

	// Directory members first so their stored metadata is not overwritten by
	// parents auto-created for an earlier file member.
	for _, dirsOnly := range []bool{true, false} {
		for _, zf := range zr.File {
			if ctxErr := ctx.Err(); ctxErr != nil {
				result.Errors = append(result.Errors, ctxErr.Error())
				writeJsonQuiet(w, r, http.StatusCreated, result)
				return
			}
			rel, ok := filer.SanitizeZipMemberName(zf)
			if !ok {
				if !dirsOnly {
					result.Errors = append(result.Errors, fmt.Sprintf("%s: unsafe member name", zf.Name))
				}
				continue
			}
			if zf.FileInfo().IsDir() != dirsOnly {
				continue
			}
			if dirsOnly {
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
			writeMember(rel, memberReader, int64(zf.UncompressedSize64), zf.Modified, mode)
			memberReader.Close()
		}
	}

	writeJsonQuiet(w, r, http.StatusCreated, result)
}

func (fs *FilerServer) mkdirExtracted(ctx context.Context, destPath string, zf *zip.File, so *operation.StorageOption) error {
	if fs.filer.FilerConf.MatchStorageRule(destPath).ReadOnly {
		return ErrReadOnly
	}
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

func (fs *FilerServer) saveExtractedEntry(ctx context.Context, destPath string, reader io.Reader, memberSize int64, fileName string, chunkSize int32, so *operation.StorageOption, mtime time.Time, mode os.FileMode) (*FilerPostResult, error) {
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

	// saveInside stores members as inline entry content like a normal
	// multipart upload; the declared size keeps a corrupt archive from
	// buffering more than it advertised.
	if so.SaveInside {
		if memberSize < 0 || memberSize > filer.ZipExtractMaxBytes {
			return nil, fmt.Errorf("zip member %s size %d out of bounds", fileName, memberSize)
		}
		var buf bytes.Buffer
		if _, err := buf.ReadFrom(io.LimitReader(reader, memberSize+1)); err != nil {
			return nil, fmt.Errorf("read member: %w", err)
		}
		if int64(buf.Len()) > memberSize {
			return nil, fmt.Errorf("zip member %s is larger than its declared size", fileName)
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
				Md5:      util.Md5(buf.Bytes()),
				FileSize: uint64(buf.Len()),
			},
			Content: buf.Bytes(),
		}
		if err := fs.filer.CreateEntry(context.WithoutCancel(ctx), entry, nil, false, false, nil, false, so.MaxFileNameLength); err != nil {
			return nil, err
		}
		return &FilerPostResult{Name: fileName, Size: int64(buf.Len())}, nil
	}

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
