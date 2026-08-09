package weed_server

import (
	"math"
	"net/http"
	"path"
	"strconv"
	"strings"

	media_hls "github.com/seaweedfs/seaweedfs/weed/media/hls"
)

const (
	hlsTsVirtualPrefix = "/hls/"
	// Keep the segment index in the internal x-seaweedfs-* namespace so normal
	// filer GETs do not expose it as an HTTP header.
	hlsTsMetadataKey = "x-seaweedfs-hls-ts-metadata"

	// Match the normal filer read prefetch concurrency.
	hlsTsSegmentPrefetch = 4

	hlsTsMiB       = int64(1024 * 1024)
	hlsTsMaxSafeMB = int64(math.MaxInt64) / hlsTsMiB
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

// hlsTsMaxChunkBytes returns the effective filer maxMB limit aligned down to a
// whole MPEG-TS packet. A request maxMB overrides the filer-wide value.
func (fs *FilerServer) hlsTsMaxChunkBytes(r *http.Request) int64 {
	var maxMB int64
	if value := r.URL.Query().Get("maxMB"); value != "" {
		parsedMaxMB, err := strconv.ParseInt(value, 10, 64)
		if err == nil && parsedMaxMB > 0 && parsedMaxMB <= hlsTsMaxSafeMB {
			maxMB = parsedMaxMB
		}
	}
	if maxMB <= 0 && fs.option != nil && fs.option.MaxMB > 0 {
		maxMB = int64(fs.option.MaxMB)
		if maxMB > hlsTsMaxSafeMB {
			// Clamp invalid configuration before converting MiB to bytes.
			maxMB = hlsTsMaxSafeMB
		}
	}
	if maxMB <= 0 {
		return 0
	}

	limit := maxMB * hlsTsMiB
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

// shouldBypassHlsTsReadJwt reports whether public HLS playback is enabled for
// this GET/HEAD request. HLS ingest always follows the filer write JWT policy.
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
