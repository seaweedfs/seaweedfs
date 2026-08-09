package weed_server

import (
	"net/http"
	"path"
	"strconv"
	"strings"

	media_hls "github.com/seaweedfs/seaweedfs/weed/media/hls"
)

const (
	hlsTsVirtualPrefix = "/hls/"
	// Use the existing x-seaweedfs-* internal metadata namespace so normal filer
	// GETs never turn the potentially large segment index into an HTTP header.
	hlsTsMetadataKey = "x-seaweedfs-hls-ts-metadata"

	// hlsTsSegmentPrefetch bounds how many chunks a multi-chunk segment read
	// fetches concurrently. It matches the value the normal filer read handler
	// uses and is capped by the segment's own chunk count.
	hlsTsSegmentPrefetch = 4
)

type hlsTsRequestKind int

const (
	hlsTsRequestInvalid hlsTsRequestKind = iota
	hlsTsRequestIngest
	hlsTsRequestPlaylist
	hlsTsRequestSegment
)

type hlsTsRequest struct {
	SourcePath string
	Kind       hlsTsRequestKind
	Sequence   int64
}

func (fs *FilerServer) hlsTsEnabled() bool {
	return fs.option != nil && fs.option.HlsTsEnabled
}

// hlsTsMaxChunkBytes follows the same maxMB semantics as normal filer
// auto-chunking: a positive request maxMB overrides the filer-wide MaxMB,
// otherwise the regular filer MaxMB is inherited. A media segment larger than
// this is stored as several chunks so no single chunk exceeds the regular filer
// chunk size; the trailing chunk holds only the remainder, and a segment read
// still fetches just the chunks that belong to that segment.
func (fs *FilerServer) hlsTsMaxChunkBytes(r *http.Request) int64 {
	parsedMaxMB, _ := strconv.ParseInt(r.URL.Query().Get("maxMB"), 10, 32)
	maxMB := int32(parsedMaxMB)
	if maxMB <= 0 && fs.option.MaxMB > 0 {
		maxMB = int32(fs.option.MaxMB)
	}
	limit := int64(maxMB) * 1024 * 1024
	// Align the limit down to a whole number of MPEG-TS packets so a chunk split
	// never cuts a packet; the segment's trailing chunk still carries the
	// remainder. A zero limit (no maxMB configured) stores one chunk per segment.
	return limit - limit%media_hls.TSPacketSize
}

func parseHlsTsRequest(requestPath string, method string) (hlsTsRequest, bool) {
	if !strings.HasPrefix(requestPath, hlsTsVirtualPrefix) {
		return hlsTsRequest{}, false
	}
	relative := strings.TrimPrefix(requestPath, hlsTsVirtualPrefix)
	if relative == "" {
		return hlsTsRequest{}, true
	}

	if method == http.MethodPost || method == http.MethodPut {
		source := "/" + strings.Trim(relative, "/")
		if source == "/" {
			return hlsTsRequest{}, true
		}
		return hlsTsRequest{SourcePath: path.Clean(source), Kind: hlsTsRequestIngest}, true
	}

	if method != http.MethodGet && method != http.MethodHead {
		return hlsTsRequest{}, true
	}
	if strings.HasSuffix(relative, "/index.m3u8") {
		source := strings.TrimSuffix(relative, "/index.m3u8")
		if source == "" {
			return hlsTsRequest{}, true
		}
		return hlsTsRequest{SourcePath: path.Clean("/" + source), Kind: hlsTsRequestPlaylist}, true
	}

	dir, file := path.Split(relative)
	if dir == "" || !strings.HasSuffix(file, ".ts") {
		return hlsTsRequest{}, true
	}
	sequence, err := strconv.ParseInt(strings.TrimSuffix(file, ".ts"), 10, 64)
	if err != nil || sequence < 0 {
		return hlsTsRequest{}, true
	}
	source := strings.TrimSuffix(dir, "/")
	if source == "" {
		return hlsTsRequest{}, true
	}
	return hlsTsRequest{SourcePath: path.Clean("/" + source), Kind: hlsTsRequestSegment, Sequence: sequence}, true
}

func hlsTsJwtSourcePath(requestPath, method string) (string, bool) {
	parsed, belongs := parseHlsTsRequest(requestPath, method)
	if !belongs || parsed.Kind == hlsTsRequestInvalid {
		return "", false
	}
	return parsed.SourcePath, true
}

// shouldBypassHlsTsReadJwt reports whether this GET/HEAD belongs to the HLS
// virtual namespace and security.toml explicitly allows HLS playback without a
// filer read JWT. HLS ingest never bypasses the normal filer write JWT policy.
func (fs *FilerServer) shouldBypassHlsTsReadJwt(r *http.Request) bool {
	if !fs.hlsTsEnabled() || fs.hlsTsReadJwtRequired.Load() {
		return false
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}
	_, belongs := parseHlsTsRequest(r.URL.Path, r.Method)
	return belongs
}

func (fs *FilerServer) maybeHandleHlsTs(w http.ResponseWriter, r *http.Request) bool {
	if !fs.hlsTsEnabled() {
		return false
	}
	parsed, belongs := parseHlsTsRequest(r.URL.Path, r.Method)
	if !belongs {
		return false
	}
	if parsed.Kind == hlsTsRequestInvalid {
		http.Error(w, "invalid HLS TS request", http.StatusBadRequest)
		return true
	}

	switch parsed.Kind {
	case hlsTsRequestIngest:
		fs.hlsTsIngestHandler(w, r, parsed.SourcePath)
	case hlsTsRequestPlaylist:
		fs.hlsTsPlaylistHandler(w, r, parsed.SourcePath)
	case hlsTsRequestSegment:
		fs.hlsTsSegmentHandler(w, r, parsed.SourcePath, parsed.Sequence)
	default:
		http.Error(w, "invalid HLS TS request", http.StatusBadRequest)
	}
	return true
}
