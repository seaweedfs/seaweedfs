// Package hlsts adapts single-file HLS MPEG-TS VOD assets: the ingest sidecar
// is an FFmpeg-style EXT-X-BYTERANGE media playlist, extents are its segments,
// and the view serves a rewritten playlist with plain numbered segment URLs.
package hlsts

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/format"
)

const (
	FormatName = "hls-ts"
	// TSPacketSize is the fixed MPEG-TS packet size; segment boundaries and
	// interior chunk cuts land on packet boundaries.
	TSPacketSize = 188

	tsSyncByte = 0x47

	PlaylistContentType = "application/vnd.apple.mpegurl"
	MediaContentType    = "video/MP2T"
)

func init() {
	format.Register(Adapter{})
}

type Adapter struct{}

var (
	_ format.Sniffer        = Adapter{}
	_ format.SidecarIndexer = Adapter{}
	_ format.Viewer         = Adapter{}
)

func (Adapter) Name() string { return FormatName }

func (Adapter) Sniff(h format.Hint) bool {
	if len(h.Head) > TSPacketSize {
		return h.Head[0] == tsSyncByte && h.Head[TSPacketSize] == tsSyncByte
	}
	return len(h.Head) > 0 && h.Head[0] == tsSyncByte
}

// playlistInfo is the adapter payload: what the generated playback playlist
// needs beyond the extent sizes.
type playlistInfo struct {
	TargetDuration int64
	MediaSequence  int64
	DurationsMs    []int64
}

func (p *playlistInfo) encode() []byte {
	out := binary.AppendUvarint(nil, uint64(p.TargetDuration))
	out = binary.AppendUvarint(out, uint64(p.MediaSequence))
	for _, durationMs := range p.DurationsMs {
		out = binary.AppendUvarint(out, uint64(durationMs))
	}
	return out
}

func decodePlaylistInfo(payload []byte, extentCount int) (*playlistInfo, error) {
	reader := bytes.NewReader(payload)
	target, err := binary.ReadUvarint(reader)
	if err != nil || target == 0 || target > math.MaxInt32 {
		return nil, fmt.Errorf("invalid hls-ts target duration")
	}
	// mirror the ingest bound: the last segment number is sequence+count-1
	sequence, err := binary.ReadUvarint(reader)
	if err != nil || sequence > math.MaxInt64-uint64(extentCount-1) {
		return nil, fmt.Errorf("invalid hls-ts media sequence")
	}
	info := &playlistInfo{TargetDuration: int64(target), MediaSequence: int64(sequence), DurationsMs: make([]int64, extentCount)}
	for i := range info.DurationsMs {
		durationMs, err := binary.ReadUvarint(reader)
		if err != nil || durationMs == 0 || durationMs > math.MaxInt32 {
			return nil, fmt.Errorf("invalid hls-ts segment %d duration", i)
		}
		info.DurationsMs[i] = int64(durationMs)
	}
	if reader.Len() != 0 {
		return nil, fmt.Errorf("hls-ts payload has trailing bytes")
	}
	return info, nil
}

// IndexSidecar parses a VOD media playlist whose segments reference one shared
// media URI through EXT-X-BYTERANGE. Playlist state the generated playback
// playlist cannot reproduce is rejected.
func (Adapter) IndexSidecar(sidecar []byte) (*format.Layout, error) {
	scanner := bufio.NewScanner(bytes.NewReader(sidecar))
	scanner.Buffer(make([]byte, 64*1024), 1<<20)

	info := &playlistInfo{}
	var sizes []int64
	var pendingDuration int64 // ms; 0 = no EXTINF pending
	var pendingSize, pendingOffset int64
	var havePendingRange bool
	var expectedOffset int64
	var mediaURI string
	var sawHeader, sawEndList bool
	var maxDurationMs int64

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		switch {
		case line == "":
		case line == "#EXTM3U":
			sawHeader = true
		case line == "#EXT-X-ENDLIST":
			sawEndList = true
		case strings.HasPrefix(line, "#EXT-X-KEY:"),
			line == "#EXT-X-DISCONTINUITY",
			strings.HasPrefix(line, "#EXT-X-DISCONTINUITY-SEQUENCE:"),
			strings.HasPrefix(line, "#EXT-X-MAP:"),
			line == "#EXT-X-GAP",
			line == "#EXT-X-I-FRAMES-ONLY":
			return nil, fmt.Errorf("%s is not supported by hls-ts ingest", strings.SplitN(line, ":", 2)[0])
		case strings.HasPrefix(line, "#EXT-X-TARGETDURATION:"):
			value := strings.TrimSpace(strings.TrimPrefix(line, "#EXT-X-TARGETDURATION:"))
			target, err := strconv.ParseInt(value, 10, 32)
			if err != nil || target <= 0 {
				return nil, fmt.Errorf("invalid EXT-X-TARGETDURATION %q", value)
			}
			info.TargetDuration = target
		case strings.HasPrefix(line, "#EXT-X-MEDIA-SEQUENCE:"):
			value := strings.TrimSpace(strings.TrimPrefix(line, "#EXT-X-MEDIA-SEQUENCE:"))
			sequence, err := strconv.ParseInt(value, 10, 64)
			if err != nil || sequence < 0 {
				return nil, fmt.Errorf("invalid EXT-X-MEDIA-SEQUENCE %q", value)
			}
			info.MediaSequence = sequence
		case strings.HasPrefix(line, "#EXTINF:"):
			if pendingDuration != 0 {
				return nil, errors.New("EXTINF without a media URI for the previous segment")
			}
			value := strings.TrimSpace(strings.TrimPrefix(line, "#EXTINF:"))
			if comma := strings.IndexByte(value, ','); comma >= 0 {
				value = value[:comma]
			}
			seconds, err := strconv.ParseFloat(value, 64)
			if err != nil || seconds <= 0 || math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds > math.MaxInt32/1000 {
				return nil, fmt.Errorf("invalid EXTINF duration %q", value)
			}
			pendingDuration = int64(math.Round(seconds * 1000))
			if pendingDuration == 0 {
				pendingDuration = 1
			}
		case strings.HasPrefix(line, "#EXT-X-BYTERANGE:"):
			if pendingDuration == 0 {
				return nil, errors.New("EXT-X-BYTERANGE without a preceding EXTINF")
			}
			value := strings.TrimSpace(strings.TrimPrefix(line, "#EXT-X-BYTERANGE:"))
			lengthText, offsetText, hasOffset := strings.Cut(value, "@")
			length, err := strconv.ParseInt(lengthText, 10, 64)
			if err != nil || length <= 0 {
				return nil, fmt.Errorf("invalid EXT-X-BYTERANGE length %q", value)
			}
			offset := expectedOffset
			if hasOffset {
				if offset, err = strconv.ParseInt(offsetText, 10, 64); err != nil || offset < 0 {
					return nil, fmt.Errorf("invalid EXT-X-BYTERANGE offset %q", value)
				}
			}
			pendingSize, pendingOffset, havePendingRange = length, offset, true
		case strings.HasPrefix(line, "#"):
			// other tags carry no state the generated playlist must keep
		default:
			if pendingDuration == 0 || !havePendingRange {
				return nil, fmt.Errorf("media URI %q without EXTINF and EXT-X-BYTERANGE", line)
			}
			if mediaURI == "" {
				mediaURI = line
			} else if mediaURI != line {
				return nil, errors.New("hls-ts ingest requires one shared media URI")
			}
			if pendingOffset != expectedOffset {
				return nil, fmt.Errorf("non-contiguous byte range at segment %d: offset %d, expected %d", len(sizes), pendingOffset, expectedOffset)
			}
			if pendingSize%TSPacketSize != 0 {
				return nil, fmt.Errorf("segment %d size %d is not a multiple of the %d-byte TS packet", len(sizes), pendingSize, TSPacketSize)
			}
			if pendingSize > math.MaxInt64-expectedOffset {
				return nil, fmt.Errorf("byte range at segment %d overflows", len(sizes))
			}
			if len(sizes) >= format.MaxExtentCount {
				return nil, fmt.Errorf("playlist has more than %d segments", format.MaxExtentCount)
			}
			sizes = append(sizes, pendingSize)
			info.DurationsMs = append(info.DurationsMs, pendingDuration)
			if pendingDuration > maxDurationMs {
				maxDurationMs = pendingDuration
			}
			expectedOffset += pendingSize
			pendingDuration, havePendingRange = 0, false
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read playlist: %w", err)
	}
	if !sawHeader {
		return nil, errors.New("playlist is missing EXTM3U")
	}
	if pendingDuration != 0 || havePendingRange {
		return nil, errors.New("playlist ended with an incomplete media segment")
	}
	if len(sizes) == 0 {
		return nil, errors.New("playlist has no media segments")
	}
	if !sawEndList {
		return nil, errors.New("only VOD playlists with EXT-X-ENDLIST are supported")
	}
	if info.MediaSequence > math.MaxInt64-int64(len(sizes)-1) {
		return nil, errors.New("EXT-X-MEDIA-SEQUENCE overflows segment numbering")
	}

	// RFC 8216: EXT-X-TARGETDURATION must be at least each segment duration
	// rounded to the nearest integer.
	minimumTarget := (maxDurationMs + 500) / 1000
	if minimumTarget < 1 {
		minimumTarget = 1
	}
	if info.TargetDuration == 0 {
		info.TargetDuration = minimumTarget
	} else if info.TargetDuration < minimumTarget {
		return nil, fmt.Errorf("EXT-X-TARGETDURATION %d is smaller than the longest segment duration %d", info.TargetDuration, minimumTarget)
	}

	layout := &format.Layout{
		Format:      FormatName,
		ExtentSizes: sizes,
		Align:       TSPacketSize,
		Payload:     info.encode(),
	}
	// valid by construction, but enforce the formattest invariant explicitly
	if err := layout.Validate(-1); err != nil {
		return nil, err
	}
	return layout, nil
}

// View serves the generated playlist, or maps ?seq=N to its extent.
func (Adapter) View(req format.ViewRequest, obj format.Object) (*format.ViewPlan, error) {
	info, err := decodePlaylistInfo(obj.Layout.Payload, len(obj.Layout.ExtentSizes))
	if err != nil {
		return nil, err
	}
	sequenceText := req.Query.Get("seq")
	if sequenceText == "" {
		return &format.ViewPlan{ContentType: PlaylistContentType, Body: renderPlaylist(obj.Name, info)}, nil
	}
	sequence, err := strconv.ParseInt(sequenceText, 10, 64)
	if err != nil || sequence < info.MediaSequence {
		return nil, format.ErrNoSuchView
	}
	index := sequence - info.MediaSequence
	if index >= int64(len(obj.Layout.ExtentSizes)) {
		return nil, format.ErrNoSuchView
	}
	return &format.ViewPlan{ContentType: MediaContentType, Extent: int(index)}, nil
}

func renderPlaylist(name string, info *playlistInfo) []byte {
	var out strings.Builder
	out.WriteString("#EXTM3U\n#EXT-X-VERSION:3\n")
	fmt.Fprintf(&out, "#EXT-X-TARGETDURATION:%d\n", info.TargetDuration)
	fmt.Fprintf(&out, "#EXT-X-MEDIA-SEQUENCE:%d\n", info.MediaSequence)
	out.WriteString("#EXT-X-PLAYLIST-TYPE:VOD\n")
	escapedName := url.PathEscape(name)
	for i, durationMs := range info.DurationsMs {
		fmt.Fprintf(&out, "#EXTINF:%.3f,\n", float64(durationMs)/1000)
		fmt.Fprintf(&out, "%s?%s=%s&seq=%d\n", escapedName, format.ViewParam, FormatName, int64(i)+info.MediaSequence)
	}
	out.WriteString("#EXT-X-ENDLIST\n")
	return []byte(out.String())
}
