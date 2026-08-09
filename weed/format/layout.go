package format

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
)

const (
	layoutVersion = 1

	// MaxExtentCount bounds decoded layouts; it also bounds the chunk count a
	// layout can force on an entry.
	MaxExtentCount = 1 << 20
	// MaxPayloadBytes bounds the adapter payload carried in entry metadata.
	MaxPayloadBytes = 16 << 20
)

// TotalSize returns the file size the layout describes.
func (l *Layout) TotalSize() int64 {
	var total int64
	for _, size := range l.ExtentSizes {
		total += size
	}
	return total
}

// ExtentRange returns the byte range of extent i.
func (l *Layout) ExtentRange(i int) (offset, size int64, ok bool) {
	if i < 0 || i >= len(l.ExtentSizes) {
		return 0, 0, false
	}
	for _, extentSize := range l.ExtentSizes[:i] {
		offset += extentSize
	}
	return offset, l.ExtentSizes[i], true
}

// Validate checks layout consistency. A negative fileSize skips the total size
// check.
func (l *Layout) Validate(fileSize int64) error {
	if l.Format == "" {
		return fmt.Errorf("layout has no format name")
	}
	if l.Align < 1 {
		return fmt.Errorf("layout align %d is invalid", l.Align)
	}
	if len(l.ExtentSizes) == 0 {
		return fmt.Errorf("layout has no extents")
	}
	if len(l.ExtentSizes) > MaxExtentCount {
		return fmt.Errorf("layout has too many extents: %d", len(l.ExtentSizes))
	}
	if len(l.Payload) > MaxPayloadBytes {
		return fmt.Errorf("layout payload is too large: %d bytes", len(l.Payload))
	}
	var total int64
	for i, size := range l.ExtentSizes {
		if size <= 0 {
			return fmt.Errorf("extent %d has invalid size %d", i, size)
		}
		if size > math.MaxInt64-total {
			return fmt.Errorf("extent %d overflows the file size", i)
		}
		total += size
	}
	if fileSize >= 0 && total != fileSize {
		return fmt.Errorf("layout describes %d bytes but the file has %d", total, fileSize)
	}
	return nil
}

// Encode serializes the layout for the LayoutKey extended attribute.
func (l *Layout) Encode() ([]byte, error) {
	if err := l.Validate(-1); err != nil {
		return nil, err
	}
	out := []byte{layoutVersion}
	out = binary.AppendUvarint(out, uint64(len(l.Format)))
	out = append(out, l.Format...)
	out = binary.AppendUvarint(out, uint64(l.Align))
	out = binary.AppendUvarint(out, uint64(len(l.ExtentSizes)))
	for _, size := range l.ExtentSizes {
		out = binary.AppendUvarint(out, uint64(size))
	}
	out = binary.AppendUvarint(out, uint64(len(l.Payload)))
	out = append(out, l.Payload...)
	return out, nil
}

// DecodeLayout parses an encoded layout and validates it.
func DecodeLayout(data []byte) (*Layout, error) {
	reader := bytes.NewReader(data)
	version, err := reader.ReadByte()
	if err != nil || version != layoutVersion {
		return nil, fmt.Errorf("unsupported layout version")
	}
	name, err := readUvarintBytes(reader, 256)
	if err != nil {
		return nil, fmt.Errorf("read layout format: %w", err)
	}
	align, err := binary.ReadUvarint(reader)
	if err != nil || align > math.MaxInt64 {
		return nil, fmt.Errorf("read layout align: invalid")
	}
	count, err := binary.ReadUvarint(reader)
	if err != nil || count > MaxExtentCount {
		return nil, fmt.Errorf("read layout extent count: invalid")
	}
	sizes := make([]int64, count)
	for i := range sizes {
		size, err := binary.ReadUvarint(reader)
		if err != nil || size > math.MaxInt64 {
			return nil, fmt.Errorf("read extent %d size: invalid", i)
		}
		sizes[i] = int64(size)
	}
	payload, err := readUvarintBytes(reader, MaxPayloadBytes)
	if err != nil {
		return nil, fmt.Errorf("read layout payload: %w", err)
	}
	if reader.Len() != 0 {
		return nil, fmt.Errorf("layout has %d trailing bytes", reader.Len())
	}
	layout := &Layout{Format: string(name), ExtentSizes: sizes, Align: int64(align), Payload: payload}
	if err := layout.Validate(-1); err != nil {
		return nil, err
	}
	return layout, nil
}

func readUvarintBytes(reader *bytes.Reader, limit uint64) ([]byte, error) {
	length, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, err
	}
	if length > limit || length > uint64(reader.Len()) {
		return nil, fmt.Errorf("length %d is out of bounds", length)
	}
	if length == 0 {
		return nil, nil
	}
	data := make([]byte, length)
	if _, err := reader.Read(data); err != nil {
		return nil, err
	}
	return data, nil
}

// Cutter yields upload chunk boundaries: every extent boundary, plus
// align-quantized cuts inside extents larger than maxChunkSize. A non-positive
// maxChunkSize keeps each extent in one chunk.
type Cutter struct {
	cuts []int64 // absolute end offset of every chunk, ascending
}

func (l *Layout) Cutter(maxChunkSize int64) *Cutter {
	quantum := maxChunkSize
	if quantum > 0 && l.Align > 1 {
		quantum -= quantum % l.Align
		if quantum <= 0 {
			// An align larger than the chunk limit still cuts on whole atoms.
			quantum = l.Align
		}
	}
	var cuts []int64
	var offset int64
	for _, size := range l.ExtentSizes {
		end := offset + size
		if quantum > 0 {
			for next := offset + quantum; next < end; next += quantum {
				cuts = append(cuts, next)
			}
		}
		cuts = append(cuts, end)
		offset = end
	}
	return &Cutter{cuts: cuts}
}

// NextChunkSize returns the size of the chunk starting at offset, or 0 past
// the end. It satisfies the filer upload loop's ChunkBoundaries interface.
func (c *Cutter) NextChunkSize(offset int64) int64 {
	i := sort.Search(len(c.cuts), func(i int) bool { return c.cuts[i] > offset })
	if i == len(c.cuts) {
		return 0
	}
	return c.cuts[i] - offset
}
