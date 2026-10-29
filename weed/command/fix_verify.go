package command

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

var (
	fixVerify          = cmdFix.Flag.Bool("verify", false, "read every needle in .dat, verify its structure and CRC32, and resynchronise past damaged regions instead of stopping at the first bad header. Needles that do not verify are left out of the rebuilt .idx and every damaged region is reported.")
	fixDryRun          = cmdFix.Flag.Bool("dryRun", false, "with -verify: only report anomalies, do not touch .idx or .dat")
	fixTruncateTail    = cmdFix.Flag.Bool("truncateTail", false, "with -verify: cut garbage after the last valid needle off the .dat (non-zero tails are saved to <dat>.tail.<offset>.bak first), so the load-time integrity check passes")
	fixReport          = cmdFix.Flag.String("report", "", "with -verify: also append the anomaly report (tab separated) to this file")
	fixMaxNeedleSizeMB = cmdFix.Flag.Int("maxNeedleSizeMB", 64, "with -verify: largest needle payload accepted while searching for the next valid needle after damage; a needle with a corrupted header larger than this cannot be recovered")
	fixWindowMB        = cmdFix.Flag.Int("windowMB", 8, "with -verify: read window used for sequential scanning")
	fixZeroWindow      = cmdFix.Flag.Int("zeroWindow", 4096, "with -verify: granularity in bytes used to report all-zero windows inside damaged regions")
)

// maxNonDataLen bounds the v2/v3 needle section that follows the payload:
// flags, name, mime, last-modified, ttl and pairs.
const maxNonDataLen = 1 + 1 + 255 + 1 + 255 + needle.LastModifiedBytesLength + needle.TtlBytesLength + 2 + 65535

var errZeroNeedleId = errors.New("zero needle id")

type datVerifyOptions struct {
	maxNeedleSize int64
	window        int
	zeroWindow    int
}

func (o *datVerifyOptions) normalize() {
	if o.zeroWindow <= 0 {
		o.zeroWindow = 4096
	}
	if o.maxNeedleSize <= 0 {
		o.maxNeedleSize = 64 << 20
	}
	if o.window < o.zeroWindow {
		o.window = o.zeroWindow
	}
	// keep the window a multiple of both the zero window and the needle alignment
	step := o.zeroWindow
	for step%types.NeedlePaddingSize != 0 {
		step++
	}
	o.window -= o.window % step
	if o.window == 0 {
		o.window = step
	}
}

type datAnomaly struct {
	Offset int64
	Length int64
	Kind   string
	Detail string
}

type datVerifySummary struct {
	FileSize    int64
	CleanEnd    int64 // end of the last valid needle
	Needles     int
	Tombstones  int
	BadRegions  int
	BadBytes    int64
	ZeroBytes   int64
	TailGarbage bool
	Warnings    int
}

// datWindow serves small reads out of a large sequential window.
type datWindow struct {
	f    *os.File
	size int64
	buf  []byte
	off  int64
	n    int
}

func (w *datWindow) readAt(p []byte, off int64) error {
	if off < 0 || off+int64(len(p)) > w.size {
		return io.ErrUnexpectedEOF
	}
	if len(p) > len(w.buf) {
		_, err := w.f.ReadAt(p, off)
		return err
	}
	if off < w.off || off+int64(len(p)) > w.off+int64(w.n) {
		n, err := w.f.ReadAt(w.buf, off)
		if err != nil && err != io.EOF {
			return err
		}
		w.off, w.n = off, n
		if off+int64(len(p)) > w.off+int64(w.n) {
			return io.ErrUnexpectedEOF
		}
	}
	copy(p, w.buf[off-w.off:])
	return nil
}

type datVerifier struct {
	f              *os.File
	vid            needle.VolumeId
	version        needle.Version
	base           int64
	size           int64
	opts           datVerifyOptions
	win            *datWindow
	body           []byte
	hdr            [types.NeedleHeaderSize]byte
	lastAppendAtNs uint64
	anomalies      []datAnomaly
	sum            datVerifySummary
}

// readNeedleAt reads and fully verifies the needle at off. When the header
// parsed but the rest did not, the partially filled needle is returned with the
// error so callers can report which file id was hit.
func (s *datVerifier) readNeedleAt(off int64) (n *needle.Needle, disk int64, err error) {
	defer func() {
		if r := recover(); r != nil {
			disk, err = 0, fmt.Errorf("needle parse panic: %v", r)
		}
	}()
	hdr := s.hdr[:]
	if err = s.win.readAt(hdr, off); err != nil {
		return nil, 0, fmt.Errorf("short header: %w", err)
	}
	n = new(needle.Needle)
	n.ParseNeedleHeader(hdr)
	if n.Id == 0 {
		return nil, 0, errZeroNeedleId
	}
	if n.Size < 0 || int64(n.Size) > s.opts.maxNeedleSize {
		return n, 0, fmt.Errorf("implausible size %d", n.Size)
	}
	disk = needle.GetActualSize(n.Size, s.version)
	if off+disk > s.size {
		return n, 0, fmt.Errorf("needle extends past end of .dat (%d > %d)", off+disk, s.size)
	}
	bodyLen := int(disk - types.NeedleHeaderSize)
	if cap(s.body) < bodyLen {
		s.body = make([]byte, bodyLen)
	}
	body := s.body[:bodyLen]
	if err = s.win.readAt(body, off+types.NeedleHeaderSize); err != nil {
		return n, 0, fmt.Errorf("read body: %w", err)
	}
	if err = n.ReadNeedleBodyBytes(body, s.version); err != nil {
		return n, 0, err
	}
	return n, disk, nil
}

// candidateOK decides whether the bytes at off look like the start of a real
// needle. Cheap header and structure checks run first so that scanning
// megabytes of garbage never triggers a full read per offset.
func (s *datVerifier) candidateOK(h *needle.Needle, chunk []byte, i int, off int64) bool {
	disk := needle.GetActualSize(h.Size, s.version)
	if off+disk > s.size {
		return false
	}
	if s.version != needle.Version1 && h.Size > 0 {
		if h.Size < 5 {
			return false
		}
		var dataSize uint32
		if i+types.NeedleHeaderSize+4 <= len(chunk) {
			dataSize = binary.BigEndian.Uint32(chunk[i+types.NeedleHeaderSize:])
		} else {
			var b [4]byte
			if _, err := s.f.ReadAt(b[:], off+types.NeedleHeaderSize); err != nil {
				return false
			}
			dataSize = binary.BigEndian.Uint32(b[:])
		}
		if int64(dataSize)+5 > int64(h.Size) || int64(h.Size)-4-int64(dataSize) > maxNonDataLen {
			return false
		}
		var flags [1]byte
		if _, err := s.f.ReadAt(flags[:], off+types.NeedleHeaderSize+4+int64(dataSize)); err != nil {
			return false
		}
		if flags[0]&0x40 != 0 { // unused flag bit
			return false
		}
	}
	n, _, err := s.readNeedleAt(off)
	if err != nil {
		return false
	}
	if len(n.Data) > 0 {
		return true // the payload CRC matched; a false positive is not realistic
	}
	// no payload to checksum (tombstone, empty needle): require the next needle to verify too
	end := off + disk
	if end == s.size || s.size-end < types.NeedleHeaderSize {
		return true
	}
	_, _, err = s.readNeedleAt(end)
	return err == nil
}

// findNext scans from the given offset at needle alignment for the next
// offset where a needle verifies. It returns -1 when none is left.
func (s *datVerifier) findNext(from int64) int64 {
	p := from
	if r := (p - s.base) % types.NeedlePaddingSize; r != 0 {
		p += types.NeedlePaddingSize - r
	}
	chunk := make([]byte, s.opts.window)
	var probe needle.Needle
	for p+types.NeedleHeaderSize <= s.size {
		n, err := s.f.ReadAt(chunk, p)
		if err != nil && err != io.EOF {
			return -1
		}
		i := 0
		for ; i+types.NeedleHeaderSize <= n; i += types.NeedlePaddingSize {
			probe.ParseNeedleHeader(chunk[i : i+types.NeedleHeaderSize])
			if probe.Id == 0 || probe.Size < 0 || int64(probe.Size) > s.opts.maxNeedleSize {
				continue
			}
			if off := p + int64(i); s.candidateOK(&probe, chunk[:n], i, off) {
				return off
			}
		}
		if n < len(chunk) || i == 0 {
			break
		}
		p += int64(i)
	}
	return -1
}

type regionStats struct {
	zeroBytes      int64
	zeroWindows    int64
	windows        int64
	longestZeroRun int64
	head           string
}

// describeRegion classifies a damaged region: how much of it is zeros, how many
// whole zeroWindow-sized blocks are zero, and the longest zero run.
func (s *datVerifier) describeRegion(start, end int64) regionStats {
	var st regionStats
	w := s.opts.zeroWindow
	chunk := make([]byte, s.opts.window)
	var run int64
	for pos := start; pos < end; {
		want := int64(len(chunk))
		if end-pos < want {
			want = end - pos
		}
		n, err := s.f.ReadAt(chunk[:want], pos)
		if n == 0 && err != nil {
			break
		}
		b := chunk[:n]
		if pos == start {
			h := b
			if len(h) > 16 {
				h = h[:16]
			}
			st.head = hex.EncodeToString(h)
		}
		for off := 0; off < len(b); off += w {
			e := off + w
			if e > len(b) {
				e = len(b)
			}
			allZero := true
			for _, c := range b[off:e] {
				if c == 0 {
					st.zeroBytes++
					run++
					continue
				}
				allZero = false
				if run > st.longestZeroRun {
					st.longestZeroRun = run
				}
				run = 0
			}
			st.windows++
			if allZero {
				st.zeroWindows++
			}
		}
		pos += int64(n)
	}
	if run > st.longestZeroRun {
		st.longestZeroRun = run
	}
	return st
}

func (s *datVerifier) recordBad(start, end int64, tail bool, cause error, hit *needle.Needle) {
	st := s.describeRegion(start, end)
	length := end - start
	kind := "garbage"
	switch {
	case st.zeroBytes == length:
		kind = "zero-fill"
	case st.zeroWindows > 0:
		kind = "garbage+zeros"
	}
	if tail {
		kind = "tail-" + kind
	}
	detail := fmt.Sprintf("cause=%q zeroBytes=%d zeroWindows=%d/%d(%dB) longestZeroRun=%d head=%s",
		cause, st.zeroBytes, st.zeroWindows, st.windows, s.opts.zeroWindow, st.longestZeroRun, st.head)
	if hit != nil && hit.Id != 0 {
		detail += fmt.Sprintf(" header=fid:%s,size:%d", needle.NewFileIdFromNeedle(s.vid, hit).String(), hit.Size)
	}
	s.anomalies = append(s.anomalies, datAnomaly{Offset: start, Length: length, Kind: kind, Detail: detail})
	s.sum.BadRegions++
	s.sum.BadBytes += length
	s.sum.ZeroBytes += st.zeroBytes
}

// verifyDatFile walks a .dat from the first needle to the end, calling visit for
// every needle that verifies. Damage never aborts the walk: the scanner looks
// for the next verifiable needle and records the skipped bytes as an anomaly.
func verifyDatFile(datName string, vid needle.VolumeId, opts datVerifyOptions, visit func(n *needle.Needle, offset int64)) (*datVerifySummary, []datAnomaly, error) {
	opts.normalize()
	f, err := os.Open(datName)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, nil, err
	}

	var sbBytes [super_block.SuperBlockSize]byte
	if _, err = f.ReadAt(sbBytes[:], 0); err != nil {
		return nil, nil, fmt.Errorf("read super block of %s: %w", datName, err)
	}
	version := needle.Version(sbBytes[0])
	if version < needle.Version1 || version > needle.Version3 {
		return nil, nil, fmt.Errorf("%s: unsupported needle version %d in super block", datName, version)
	}
	base := int64(super_block.SuperBlockSize)
	if version != needle.Version1 {
		base += int64(binary.BigEndian.Uint16(sbBytes[6:8]))
	}

	s := &datVerifier{
		f: f, vid: vid, version: version, base: base, size: st.Size(), opts: opts,
		win: &datWindow{f: f, size: st.Size(), buf: make([]byte, opts.window)},
	}
	s.sum.FileSize = s.size

	pos := base
	for pos < s.size {
		if s.size-pos < types.NeedleHeaderSize {
			s.recordBad(pos, s.size, true, errors.New("partial header at end of file"), nil)
			s.sum.TailGarbage = true
			break
		}
		n, disk, err := s.readNeedleAt(pos)
		if err == nil {
			if n.Size.IsValid() {
				s.sum.Needles++
			} else {
				s.sum.Tombstones++
			}
			if n.AppendAtNs != 0 {
				if n.AppendAtNs < s.lastAppendAtNs {
					s.anomalies = append(s.anomalies, datAnomaly{Offset: pos, Kind: "timestamp-regression",
						Detail: fmt.Sprintf("appendAtNs %d is older than the previous needle (%d)", n.AppendAtNs, s.lastAppendAtNs)})
					s.sum.Warnings++
				}
				s.lastAppendAtNs = n.AppendAtNs
			}
			visit(n, pos)
			pos += disk
			continue
		}
		next := s.findNext(pos + types.NeedlePaddingSize)
		if next < 0 {
			s.recordBad(pos, s.size, true, err, n)
			s.sum.TailGarbage = true
			break
		}
		s.recordBad(pos, next, false, err, n)
		pos = next
	}
	s.sum.CleanEnd = pos
	return &s.sum, s.anomalies, nil
}

func truncateDatTail(datName string, cleanEnd int64) error {
	f, err := os.OpenFile(datName, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if st.Size() <= cleanEnd {
		return nil
	}
	// keep the cut-off bytes unless they are all zeros
	buf := make([]byte, 1<<20)
	nonZero := false
	for off := cleanEnd; off < st.Size() && !nonZero; {
		n, rerr := f.ReadAt(buf, off)
		for _, c := range buf[:n] {
			if c != 0 {
				nonZero = true
				break
			}
		}
		off += int64(n)
		if rerr != nil {
			break
		}
	}
	if nonZero {
		bak := fmt.Sprintf("%s.tail.%d.bak", datName, cleanEnd)
		out, err := os.OpenFile(bak, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
		if err != nil {
			return fmt.Errorf("create tail backup %s: %w", bak, err)
		}
		_, err = io.Copy(out, io.NewSectionReader(f, cleanEnd, st.Size()-cleanEnd))
		if cerr := out.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return fmt.Errorf("write tail backup %s: %w", bak, err)
		}
		fmt.Printf("saved %d bytes cut from the tail to %s\n", st.Size()-cleanEnd, bak)
	}
	return f.Truncate(cleanEnd)
}

func doFixOneVolumeVerified(basepath string, baseFileName string, volumeId int64, indexFileName string) {
	if *fixRemoteFile {
		glog.Fatalf("-verify reads the local .dat and cannot be combined with -remoteFile")
	}
	if volumeId < 0 || volumeId > 0xFFFFFFFF {
		glog.Fatalf("volume ID out of range: %d", volumeId)
	}
	vid := needle.VolumeId(volumeId)
	datName := path.Join(basepath, baseFileName+".dat")

	nm := needle_map.NewMemDb()
	nmDeleted := needle_map.NewMemDb()
	defer nm.Close()
	defer nmDeleted.Close()
	scanner := &VolumeFileScanner4Fix{nm: nm, nmDeleted: nmDeleted, includeDeleted: *fixIncludeDeleted}

	opts := datVerifyOptions{
		maxNeedleSize: int64(*fixMaxNeedleSizeMB) << 20,
		window:        *fixWindowMB << 20,
		zeroWindow:    *fixZeroWindow,
	}
	started := time.Now()
	var visitErr error
	sum, anomalies, err := verifyDatFile(datName, vid, opts, func(n *needle.Needle, offset int64) {
		if visitErr == nil {
			// the version only matters for DiskSize in a debug log line
			visitErr = scanner.VisitNeedle(n, offset, nil, nil)
		}
	})
	if err != nil {
		glog.Fatalf("verify %s: %v", datName, err)
	}
	if visitErr != nil {
		glog.Fatalf("verify %s: %v", datName, visitErr)
	}

	var reportFile *os.File
	if *fixReport != "" {
		if reportFile, err = os.OpenFile(*fixReport, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644); err != nil {
			glog.Fatalf("open report %s: %v", *fixReport, err)
		}
		defer reportFile.Close()
	}
	for _, a := range anomalies {
		line := fmt.Sprintf("volume=%d\tkind=%s\toffset=%d\tlength=%d\t%s", volumeId, a.Kind, a.Offset, a.Length, a.Detail)
		fmt.Println(line)
		if reportFile != nil {
			fmt.Fprintln(reportFile, line)
		}
	}
	fmt.Printf("volume %d: %d needles + %d tombstones verified in %v; %d damaged regions (%d bytes, %d of them zero), %d warnings; clean end %d of %d\n",
		volumeId, sum.Needles, sum.Tombstones, time.Since(started).Round(time.Millisecond),
		sum.BadRegions, sum.BadBytes, sum.ZeroBytes, sum.Warnings, sum.CleanEnd, sum.FileSize)

	if *fixDryRun {
		return
	}
	if sum.TailGarbage {
		if *fixTruncateTail {
			if err := truncateDatTail(datName, sum.CleanEnd); err != nil {
				glog.Fatalf("truncate %s: %v", datName, err)
			}
			fmt.Printf("volume %d: truncated %s to %d bytes\n", volumeId, datName, sum.CleanEnd)
		} else {
			fmt.Printf("volume %d: WARNING %d bytes follow the last valid needle; the volume server keeps it read-only until the tail is cut (rerun with -truncateTail)\n",
				volumeId, sum.FileSize-sum.CleanEnd)
		}
	}

	// keep the old index around: a rebuilt index only knows what survived in .dat
	if _, statErr := os.Stat(indexFileName); statErr == nil {
		bak := indexFileName + ".bak"
		if _, statErr := os.Stat(bak); statErr == nil {
			bak = fmt.Sprintf("%s.bak.%d", indexFileName, time.Now().Unix())
		}
		if err := os.Rename(indexFileName, bak); err != nil {
			glog.Fatalf("back up %s: %v", indexFileName, err)
		}
		fmt.Printf("volume %d: previous index saved as %s\n", volumeId, bak)
	}
	if err := SaveToIdx(scanner, indexFileName); err != nil {
		glog.Fatalf("save to .idx File: %v", strings.TrimSpace(err.Error()))
	}
}
