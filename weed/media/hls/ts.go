// Package hls provides helpers for chunk-aligned HLS MPEG-TS VOD metadata.
package hls

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	// MetadataVersion identifies the persisted HLS TS metadata format.
	MetadataVersion = 1
	// MaxPlaylistBytes is the maximum accepted ingest playlist size.
	MaxPlaylistBytes = 16 * 1024 * 1024
	// TSPacketSize is the fixed MPEG-TS packet size in bytes.
	TSPacketSize = 188

	tsSyncByte = 0x47

	maxInt64Value int64 = 1<<63 - 1
)

// Metadata describes the HLS segment layout stored with a filer entry.
type Metadata struct {
	Version        int       `json:"version"`
	TargetDuration int       `json:"target_duration"`
	MediaSequence  int64     `json:"media_sequence,omitempty"`
	Segments       []Segment `json:"segments"`
}

// Segment describes one MPEG-TS segment within the logical media file.
type Segment struct {
	Offset   int64   `json:"offset"`
	Size     int64   `json:"size"`
	Duration float64 `json:"duration"`
}

// ParseSingleFilePlaylist parses a VOD HLS media playlist whose segments use
// EXT-X-BYTERANGE references to one shared media URI. Tags whose state cannot
// be preserved in the generated playback playlist are rejected.
func ParseSingleFilePlaylist(data []byte) (*Metadata, error) {
	if len(data) > MaxPlaylistBytes {
		return nil, fmt.Errorf("playlist is too large: %d bytes", len(data))
	}

	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 64*1024), MaxPlaylistBytes)

	metadata := &Metadata{Version: MetadataVersion}
	var pendingDuration *float64
	var pendingSize int64
	var pendingOffset int64
	var havePendingRange bool
	var expectedOffset int64
	var mediaURI string
	var sawHeader bool
	var sawEndList bool
	var maxDuration float64

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		switch {
		case line == "#EXTM3U":
			sawHeader = true
			continue
		case line == "#EXT-X-ENDLIST":
			sawEndList = true
			continue
		case strings.HasPrefix(line, "#EXT-X-KEY:"):
			return nil, errors.New("EXT-X-KEY is not supported by HLS TS VOD ingest")
		case line == "#EXT-X-DISCONTINUITY" || strings.HasPrefix(line, "#EXT-X-DISCONTINUITY-SEQUENCE:"):
			return nil, errors.New("EXT-X-DISCONTINUITY is not supported by HLS TS VOD ingest")
		case strings.HasPrefix(line, "#EXT-X-MAP:"):
			return nil, errors.New("EXT-X-MAP is not supported by HLS TS VOD ingest")
		case line == "#EXT-X-GAP":
			return nil, errors.New("EXT-X-GAP is not supported by HLS TS VOD ingest")
		case line == "#EXT-X-I-FRAMES-ONLY":
			return nil, errors.New("EXT-X-I-FRAMES-ONLY is not supported by HLS TS VOD ingest")
		case strings.HasPrefix(line, "#EXT-X-TARGETDURATION:"):
			value := strings.TrimSpace(strings.TrimPrefix(line, "#EXT-X-TARGETDURATION:"))
			target, err := strconv.Atoi(value)
			if err != nil || target <= 0 {
				return nil, fmt.Errorf("invalid EXT-X-TARGETDURATION %q", value)
			}
			metadata.TargetDuration = target
			continue
		case strings.HasPrefix(line, "#EXT-X-MEDIA-SEQUENCE:"):
			value := strings.TrimSpace(strings.TrimPrefix(line, "#EXT-X-MEDIA-SEQUENCE:"))
			sequence, err := strconv.ParseInt(value, 10, 64)
			if err != nil || sequence < 0 {
				return nil, fmt.Errorf("invalid EXT-X-MEDIA-SEQUENCE %q", value)
			}
			metadata.MediaSequence = sequence
			continue
		case strings.HasPrefix(line, "#EXTINF:"):
			if pendingDuration != nil {
				return nil, errors.New("EXTINF without a media URI for the previous segment")
			}
			value := strings.TrimSpace(strings.TrimPrefix(line, "#EXTINF:"))
			if comma := strings.IndexByte(value, ','); comma >= 0 {
				value = value[:comma]
			}
			duration, err := strconv.ParseFloat(value, 64)
			if err != nil || duration <= 0 || math.IsNaN(duration) || math.IsInf(duration, 0) {
				return nil, fmt.Errorf("invalid EXTINF duration %q", value)
			}
			pendingDuration = &duration
			if duration > maxDuration {
				maxDuration = duration
			}
			continue
		case strings.HasPrefix(line, "#EXT-X-BYTERANGE:"):
			if havePendingRange {
				return nil, errors.New("EXT-X-BYTERANGE without a media URI for the previous segment")
			}
			value := strings.TrimSpace(strings.TrimPrefix(line, "#EXT-X-BYTERANGE:"))
			sizeText, offsetText, hasOffset := strings.Cut(value, "@")
			size, err := strconv.ParseInt(strings.TrimSpace(sizeText), 10, 64)
			if err != nil || size <= 0 {
				return nil, fmt.Errorf("invalid EXT-X-BYTERANGE size %q", sizeText)
			}
			offset := expectedOffset
			if hasOffset {
				offset, err = strconv.ParseInt(strings.TrimSpace(offsetText), 10, 64)
				if err != nil || offset < 0 {
					return nil, fmt.Errorf("invalid EXT-X-BYTERANGE offset %q", offsetText)
				}
			}
			pendingSize = size
			pendingOffset = offset
			havePendingRange = true
			continue
		case strings.HasPrefix(line, "#"):
			continue
		}

		if pendingDuration == nil || !havePendingRange {
			return nil, fmt.Errorf("media URI %q is missing EXTINF or EXT-X-BYTERANGE", line)
		}
		if mediaURI == "" {
			mediaURI = line
		} else if line != mediaURI {
			return nil, fmt.Errorf("playlist is not single-file HLS: media URI changed from %q to %q", mediaURI, line)
		}
		if pendingOffset != expectedOffset {
			return nil, fmt.Errorf("non-contiguous byte range at segment %d: got offset %d, expected %d", len(metadata.Segments), pendingOffset, expectedOffset)
		}
		if pendingSize > maxInt64Value-pendingOffset {
			return nil, fmt.Errorf("byte range at segment %d overflows int64", len(metadata.Segments))
		}

		metadata.Segments = append(metadata.Segments, Segment{
			Offset:   pendingOffset,
			Size:     pendingSize,
			Duration: *pendingDuration,
		})
		expectedOffset = pendingOffset + pendingSize
		pendingDuration = nil
		havePendingRange = false
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read playlist: %w", err)
	}
	if !sawHeader {
		return nil, errors.New("playlist is missing EXTM3U")
	}
	if pendingDuration != nil || havePendingRange {
		return nil, errors.New("playlist ended with an incomplete media segment")
	}
	if len(metadata.Segments) == 0 {
		return nil, errors.New("playlist has no media segments")
	}
	if !sawEndList {
		return nil, errors.New("only VOD playlists with EXT-X-ENDLIST are supported")
	}
	if metadata.MediaSequence > maxInt64Value-int64(len(metadata.Segments)-1) {
		return nil, errors.New("EXT-X-MEDIA-SEQUENCE overflows segment numbering")
	}

	// RFC 8216 requires EXT-X-TARGETDURATION to be at least each EXTINF
	// duration rounded to the nearest integer.
	minimumTarget := int(math.Floor(maxDuration + 0.5))
	if minimumTarget < 1 {
		minimumTarget = 1
	}
	if metadata.TargetDuration == 0 {
		metadata.TargetDuration = minimumTarget
	} else if metadata.TargetDuration < minimumTarget {
		return nil, fmt.Errorf("EXT-X-TARGETDURATION %d is smaller than maximum rounded segment duration %d", metadata.TargetDuration, minimumTarget)
	}

	return metadata, nil
}

// Validate checks metadata consistency. A negative fileSize skips the total
// size check, and a non-positive maxSegmentSize disables the segment size limit.
func Validate(metadata *Metadata, fileSize, maxSegmentSize int64) error {
	if metadata == nil || metadata.Version != MetadataVersion {
		return errors.New("unsupported HLS TS metadata version")
	}
	if metadata.TargetDuration <= 0 || len(metadata.Segments) == 0 {
		return errors.New("invalid HLS TS metadata")
	}
	if metadata.MediaSequence < 0 || metadata.MediaSequence > maxInt64Value-int64(len(metadata.Segments)-1) {
		return errors.New("invalid HLS TS media sequence")
	}
	var expectedOffset int64
	for i, segment := range metadata.Segments {
		if segment.Offset != expectedOffset {
			return fmt.Errorf("segment %d offset %d does not follow %d", i, segment.Offset, expectedOffset)
		}
		if segment.Size <= 0 {
			return fmt.Errorf("segment %d has invalid size %d", i, segment.Size)
		}
		if segment.Size > maxInt64Value-expectedOffset {
			return fmt.Errorf("segment %d byte range overflows int64", i)
		}
		if maxSegmentSize > 0 && segment.Size > maxSegmentSize {
			return fmt.Errorf("segment %d size %d exceeds maximum %d", i, segment.Size, maxSegmentSize)
		}
		if segment.Duration <= 0 || math.IsNaN(segment.Duration) || math.IsInf(segment.Duration, 0) {
			return fmt.Errorf("segment %d has invalid duration", i)
		}
		expectedOffset += segment.Size
	}
	if fileSize >= 0 && expectedOffset != fileSize {
		return fmt.Errorf("playlist describes %d bytes but media file has %d", expectedOffset, fileSize)
	}
	return nil
}

// ValidateTSPackets verifies that data contains only complete MPEG-TS packets
// with the expected sync byte at each packet boundary.
func ValidateTSPackets(data []byte) error {
	if len(data)%TSPacketSize != 0 {
		return fmt.Errorf("media is not MPEG-TS: %d bytes is not a multiple of the %d-byte packet size", len(data), TSPacketSize)
	}
	for offset := 0; offset < len(data); offset += TSPacketSize {
		if data[offset] != tsSyncByte {
			return fmt.Errorf("media is not MPEG-TS: missing 0x47 sync byte at packet offset %d", offset)
		}
	}
	return nil
}

// RenderMediaPlaylist renders metadata as a VOD playlist with numbered .ts URLs.
func RenderMediaPlaylist(metadata *Metadata) []byte {
	var out strings.Builder
	out.WriteString("#EXTM3U\n")
	out.WriteString("#EXT-X-VERSION:3\n")
	fmt.Fprintf(&out, "#EXT-X-TARGETDURATION:%d\n", metadata.TargetDuration)
	fmt.Fprintf(&out, "#EXT-X-MEDIA-SEQUENCE:%d\n", metadata.MediaSequence)
	out.WriteString("#EXT-X-PLAYLIST-TYPE:VOD\n")
	for i, segment := range metadata.Segments {
		fmt.Fprintf(&out, "#EXTINF:%.3f,\n%d.ts\n", segment.Duration, int64(i)+metadata.MediaSequence)
	}
	out.WriteString("#EXT-X-ENDLIST\n")
	return []byte(out.String())
}
