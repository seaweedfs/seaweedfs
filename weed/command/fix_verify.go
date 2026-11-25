package command

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	fixVerify       = cmdFix.Flag.Bool("verify", false, "read every needle in .dat, verify its structure and CRC32, and resynchronise past damaged regions instead of stopping at the first bad header. Needles that do not verify are left out of the rebuilt .idx and every damaged region is reported.")
	fixDryRun       = cmdFix.Flag.Bool("dryRun", false, "with -verify: only report anomalies, do not touch .idx or .dat")
	fixTruncateTail = cmdFix.Flag.Bool("truncateTail", false, "with -verify: cut garbage after the last valid needle off the .dat (non-zero tails are saved to <dat>.tail.<offset>.bak first), so the load-time integrity check passes")
	fixMaxNeedleMB  = cmdFix.Flag.Int("maxNeedleSizeMB", 64, "with -verify: largest needle payload accepted while searching for the next valid needle after damage; a needle with a corrupted header larger than this cannot be recovered")
)

// printAnomalies writes one tab separated line per damaged region.
func printAnomalies(volumeId int64, anomalies []datAnomaly) {
	for _, a := range anomalies {
		fmt.Printf("volume=%d\tkind=%s\toffset=%d\tlength=%d\t%s\n", volumeId, a.Kind, a.Offset, a.Length, a.Detail)
	}
}

func printVerifySummary(volumeId int64, sum *datVerifySummary, took time.Duration) {
	fmt.Printf("volume %d: %d needles + %d tombstones verified in %v; %d damaged regions (%d bytes, %d of them zero), %d warnings; clean end %d of %d\n",
		volumeId, sum.Needles, sum.Tombstones, took.Round(time.Millisecond),
		sum.BadRegions, sum.BadBytes, sum.ZeroBytes, sum.Warnings, sum.CleanEnd, sum.FileSize)
}

// maxNonDataLen bounds the v2/v3 needle section that follows the payload:
// flags, name, mime, last-modified, ttl and pairs.
const maxNonDataLen = 1 + 1 + 255 + 1 + 255 + needle.LastModifiedBytesLength + needle.TtlBytesLength + 2 + 65535

var errZeroNeedleId = errors.New("zero needle id")

type datVerifyOptions struct {
	maxNeedleSize int64
	window        int
	zeroWindow    int
	// Progress, when set, is called every few thousand needles with the current offset.
	Progress func(pos, total int64)
}

// datVisitor receives the events of one scan. raw header and body are only valid during the call.
type datVisitor struct {
	needle func(n *needle.Needle, offset int64, header, body []byte) error
	// damage is called for every skipped region [start, end); tail is true when nothing valid follows.
	damage func(start, end int64, tail bool) error
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
	// DamagedRecords are the records whose header still parsed inside a damaged region.
	DamagedRecords []damagedRecord
}

// damagedRecord is a key whose record at Offset could not be verified.
type damagedRecord struct {
	Key    types.NeedleId
	Offset int64
}

// staleVersionWarnings reports keys whose newest record was damaged while an
// older copy of the same key made it into the rebuilt index.
func staleVersionWarnings(nm *needle_map.MemDb, damaged []damagedRecord) []datAnomaly {
	var warnings []datAnomaly
	for _, d := range damaged {
		v, ok := nm.Get(d.Key)
		if !ok || v.Offset.ToActualOffset() >= d.Offset {
			continue
		}
		warnings = append(warnings, datAnomaly{Offset: d.Offset, Kind: "stale-version",
			Detail: fmt.Sprintf("key=%s: the record at %d is damaged, the rebuilt index points at the older copy at %d", d.Key, d.Offset, v.Offset.ToActualOffset())})
	}
	return warnings
}

// datWindow serves small reads out of a large sequential window.
type datWindow struct {
	f    io.ReaderAt
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
	f              io.ReaderAt
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
// error so callers can report which file id was hit. bounded applies
// -maxNeedleSizeMB, which only limits guesses made while resynchronising: a
// record reached at a known boundary may be as large as the file allows.
func (s *datVerifier) readNeedleAt(off int64, bounded bool) (n *needle.Needle, disk int64, err error) {
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
	if n.Size < 0 || (bounded && int64(n.Size) > s.opts.maxNeedleSize) {
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
	if err = checkRecordFields(n, s.version); err != nil {
		return n, 0, err
	}
	return n, disk, nil
}

// checkRecordFields rejects a v2/v3 record whose parsed fields do not add up
// to its size. The writer only stores the data length and the fields after it
// when there is data, so a live record always has data, and the CRC check
// only runs when it does: a zero fill over the data length would otherwise
// pass as an empty file.
func checkRecordFields(n *needle.Needle, version needle.Version) error {
	if version == needle.Version1 || n.Size <= 0 {
		return nil
	}
	if n.DataSize == 0 {
		return fmt.Errorf("record of size %d carries no data", n.Size)
	}
	fields := 4 + int64(n.DataSize) + 1
	if n.HasName() {
		fields += 1 + int64(n.NameSize)
	}
	if n.HasMime() {
		fields += 1 + int64(n.MimeSize)
	}
	if n.HasLastModifiedDate() {
		fields += needle.LastModifiedBytesLength
	}
	if n.HasTtl() {
		fields += needle.TtlBytesLength
	}
	if n.HasPairs() {
		fields += 2 + int64(n.PairsSize)
	}
	if fields != int64(n.Size) {
		return fmt.Errorf("record size %d does not match its fields (%d bytes)", n.Size, fields)
	}
	return nil
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
	n, disk, err := s.readNeedleAt(off, true)
	if err != nil {
		return false
	}
	if len(n.Data) > 0 {
		return true // the payload CRC matched; a false positive is not realistic
	}
	// No payload to checksum: only a delete record (size 0) gets here, since a
	// live record without data fails checkRecordFields. A v3 delete record
	// carries its append time: one that is believable, not older than the last
	// verified record and followed by zero padding stands on its own, so a
	// delete is not lost when the bytes after it are damaged too. Otherwise the
	// record needs the end of the file or a verifiable successor.
	end := off + disk
	if h.Size == 0 {
		stamped := s.version != needle.Version3 || n.AppendAtNs != 0
		if s.version == needle.Version3 && n.AppendAtNs != 0 {
			if !plausibleAppendAtNs(n.AppendAtNs) {
				return false
			}
			if n.AppendAtNs >= s.lastAppendAtNs && allZero(s.body[needle.NeedleChecksumSize+types.TimestampSize:disk-types.NeedleHeaderSize]) {
				return true
			}
		}
		if end == s.size || (stamped && s.size-end < types.NeedleHeaderSize) {
			return true
		}
	}
	_, _, err = s.readNeedleAt(end, false)
	return err == nil
}

func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// plausibleAppendAtNs tells a real append timestamp (v3 needles) from garbage.
func plausibleAppendAtNs(ns uint64) bool {
	return ns >= uint64(time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()) &&
		ns <= uint64(time.Now().Add(48*time.Hour).UnixNano())
}

// findNext scans from the given offset at needle alignment for the next
// offset where a needle verifies. It returns -1 when none is left.
func (s *datVerifier) findNext(from int64) (int64, error) {
	p := from
	if r := (p - s.base) % types.NeedlePaddingSize; r != 0 {
		p += types.NeedlePaddingSize - r
	}
	chunk := make([]byte, s.opts.window)
	var probe needle.Needle
	for p+types.NeedleHeaderSize <= s.size {
		n, err := s.f.ReadAt(chunk, p)
		if err != nil && err != io.EOF {
			return -1, fmt.Errorf("read at %d while searching for the next needle: %w", p, err)
		}
		i := 0
		for ; i+types.NeedleHeaderSize <= n; i += types.NeedlePaddingSize {
			probe.ParseNeedleHeader(chunk[i : i+types.NeedleHeaderSize])
			if probe.Id == 0 || probe.Size < 0 || int64(probe.Size) > s.opts.maxNeedleSize {
				continue
			}
			if off := p + int64(i); s.candidateOK(&probe, chunk[:n], i, off) {
				return off, nil
			}
		}
		if n < len(chunk) || i == 0 {
			break
		}
		p += int64(i)
	}
	return -1, nil
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
		s.sum.DamagedRecords = append(s.sum.DamagedRecords, damagedRecord{Key: hit.Id, Offset: start})
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
	return verifyDatFileWith(datName, vid, opts, &datVisitor{needle: func(n *needle.Needle, offset int64, _, _ []byte) error {
		visit(n, offset)
		return nil
	}})
}

func verifyDatFileWith(datName string, vid needle.VolumeId, opts datVerifyOptions, v *datVisitor) (*datVerifySummary, []datAnomaly, error) {
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

	// the DiskFile wrapper only reads the super block; f stays ours and is closed above
	sb, err := super_block.ReadSuperBlock(backend.NewDiskFile(f))
	if err != nil {
		return nil, nil, err
	}
	version := sb.Version
	if version < needle.Version1 || version > needle.Version3 {
		return nil, nil, fmt.Errorf("%s: unsupported needle version %d in super block", datName, version)
	}
	// A super block extra describes an erasure-coded volume layout; its .dat was
	// re-striped into .ecXX shards and is not a plain needle file any more.
	if sb.ExtraSize > 0 || sb.Extra != nil {
		return nil, nil, fmt.Errorf("%s: super block extra (erasure-coded layout) is not supported", datName)
	}
	base := int64(sb.BlockSize())

	s := &datVerifier{
		f: f, vid: vid, version: version, base: base, size: st.Size(), opts: opts,
		win: &datWindow{f: f, size: st.Size(), buf: make([]byte, opts.window)},
	}
	s.sum.FileSize = s.size

	damage := func(start, end int64, tail bool) error {
		if v.damage == nil {
			return nil
		}
		return v.damage(start, end, tail)
	}

	pos := base
	for count := 0; pos < s.size; count++ {
		if opts.Progress != nil && count&0xfff == 0 {
			opts.Progress(pos, s.size)
		}
		if s.size-pos < types.NeedleHeaderSize {
			s.recordBad(pos, s.size, true, errors.New("partial header at end of file"), nil)
			s.sum.TailGarbage = true
			if err := damage(pos, s.size, true); err != nil {
				return nil, nil, err
			}
			break
		}
		n, disk, err := s.readNeedleAt(pos, false)
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
			if v.needle != nil {
				if err := v.needle(n, pos, s.hdr[:], s.body[:disk-types.NeedleHeaderSize]); err != nil {
					return nil, nil, err
				}
			}
			pos += disk
			continue
		}
		next, ferr := s.findNext(pos + types.NeedlePaddingSize)
		if ferr != nil {
			return nil, nil, ferr
		}
		if next < 0 {
			s.recordBad(pos, s.size, true, err, n)
			s.sum.TailGarbage = true
			if derr := damage(pos, s.size, true); derr != nil {
				return nil, nil, derr
			}
			break
		}
		s.recordBad(pos, next, false, err, n)
		if derr := damage(pos, next, false); derr != nil {
			return nil, nil, derr
		}
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
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			// a tail that cannot be read cannot be shown to be zeros, so it is not cut
			return fmt.Errorf("read the tail at %d: %w", off, rerr)
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
			os.Remove(bak)
			return fmt.Errorf("write tail backup %s: %w", bak, err)
		}
		fmt.Printf("saved %d bytes cut from the tail to %s\n", st.Size()-cleanEnd, bak)
	}
	return f.Truncate(cleanEnd)
}

// doFixOneVolumeVerified rebuilds the .idx of one volume from the needles that
// verify, resynchronising past damaged regions. Errors are returned so the
// caller can honour -ignoreError and move on to the next volume.
func doFixOneVolumeVerified(basepath string, baseFileName string, volumeId int64, indexFileName string) error {
	if *fixRemoteFile {
		return fmt.Errorf("-verify reads the local .dat and cannot be combined with -remoteFile")
	}
	vid, err := needle.NewVolumeId(strconv.FormatInt(volumeId, 10))
	if err != nil {
		return fmt.Errorf("volume ID out of range: %d", volumeId)
	}
	datName := path.Join(basepath, baseFileName+".dat")

	// A .cpc marker means an interrupted vacuum commit is pending: the volume
	// server reconciles it on its next start (roll forward with the marker, roll
	// back orphan .cpd/.cpx without it). Writing a rebuilt .idx here would race
	// that decision, so make the operator settle it first.
	if cpcPath := strings.TrimSuffix(datName, ".dat") + ".cpc"; util.FileExists(cpcPath) {
		return fmt.Errorf("volume %d: a compaction commit is pending (%s); start the volume server once so it reconciles the interrupted vacuum, then re-run", volumeId, cpcPath)
	}

	nm := needle_map.NewMemDb()
	nmDeleted := needle_map.NewMemDb()
	defer nm.Close()
	defer nmDeleted.Close()
	scanner := &VolumeFileScanner4Fix{nm: nm, nmDeleted: nmDeleted, includeDeleted: *fixIncludeDeleted}

	opts := datVerifyOptions{maxNeedleSize: int64(*fixMaxNeedleMB) << 20, window: 8 << 20, zeroWindow: 4096, Progress: newProgressPrinter(volumeId)}
	visitor := &datVisitor{needle: func(n *needle.Needle, offset int64, header, body []byte) error {
		// the version only matters for DiskSize in a debug log line
		return scanner.VisitNeedle(n, offset, nil, nil)
	}}
	started := time.Now()
	sum, anomalies, err := verifyDatFileWith(datName, vid, opts, visitor)
	if err != nil {
		return fmt.Errorf("verify %s: %w", datName, err)
	}
	stale := staleVersionWarnings(nm, sum.DamagedRecords)
	sum.Warnings += len(stale)
	anomalies = append(anomalies, stale...)

	printAnomalies(volumeId, anomalies)
	printVerifySummary(volumeId, sum, time.Since(started))

	if *fixDryRun {
		return nil
	}
	if sum.TailGarbage {
		if *fixTruncateTail {
			if err := truncateDatTail(datName, sum.CleanEnd); err != nil {
				return fmt.Errorf("truncate %s: %w", datName, err)
			}
			fmt.Printf("volume %d: truncated %s to %d bytes\n", volumeId, datName, sum.CleanEnd)
		} else {
			fmt.Printf("volume %d: WARNING %d bytes follow the last valid needle; the volume server keeps it read-only until the tail is cut (rerun with -truncateTail)\n",
				volumeId, sum.FileSize-sum.CleanEnd)
		}
	}
	if err := installRebuiltIndex(scanner, indexFileName, volumeId); err != nil {
		return err
	}
	printIdxDirHint(volumeId)
	return nil
}

// installRebuiltIndex writes the rebuilt index to a staging file first and
// swaps it in only once it is complete, keeping the previous index as .bak (a
// rebuilt index only knows what survived in .dat). A .ldb cache next to the
// index describes the old index, so it is dropped and the volume server
// rebuilds it from the new one.
func installRebuiltIndex(scanner *VolumeFileScanner4Fix, indexFileName string, volumeId int64) error {
	staged := indexFileName + ".tmp"
	if err := SaveToIdx(scanner, staged); err != nil {
		os.Remove(staged)
		return fmt.Errorf("save to .idx File: %v", strings.TrimSpace(err.Error()))
	}
	if _, statErr := os.Stat(indexFileName); statErr == nil {
		bak := backupName(indexFileName)
		if err := os.Rename(indexFileName, bak); err != nil {
			os.Remove(staged)
			return fmt.Errorf("back up %s: %w", indexFileName, err)
		}
		fmt.Printf("volume %d: previous index saved as %s\n", volumeId, bak)
	}
	if err := os.Rename(staged, indexFileName); err != nil {
		return fmt.Errorf("install %s as %s (the previous index is in its .bak): %w", staged, indexFileName, err)
	}
	if err := util.FsyncDir(path.Dir(indexFileName)); err != nil {
		glog.Warningf("sync directory of %s: %v", indexFileName, err)
	}
	if ldb := strings.TrimSuffix(indexFileName, ".idx") + ".ldb"; util.FileExists(ldb) || util.FolderExists(ldb) {
		if err := os.RemoveAll(ldb); err != nil {
			return fmt.Errorf("remove the stale %s (delete it before starting the volume server): %w", ldb, err)
		}
		fmt.Printf("volume %d: removed the stale %s; the volume server rebuilds it from the new index\n", volumeId, ldb)
	}
	return nil
}

// printIdxDirHint reminds the operator that a volume server started with a
// separate -dir.idx directory keeps its own .idx there: this command writes the
// rebuilt index next to the .dat, so the file must be moved over (and any stale
// .ldb dropped) or the server keeps using the old index.
func printIdxDirHint(volumeId int64) {
	fmt.Printf("volume %d: the rebuilt index was written next to the .dat; if the server uses a separate -dir.idx directory, replace the .idx there and drop any stale .ldb\n", volumeId)
}

// backupName returns path.bak, or path.bak.<unix time> when that already exists.
func backupName(path string) string {
	bak := path + ".bak"
	if _, err := os.Stat(bak); err == nil {
		bak = fmt.Sprintf("%s.bak.%d", path, time.Now().Unix())
	}
	return bak
}

func newProgressPrinter(volumeId int64) func(pos, total int64) {
	started := time.Now()
	last := started
	return func(pos, total int64) {
		now := time.Now()
		if now.Sub(last) < 30*time.Second {
			return
		}
		last = now
		fmt.Fprintf(os.Stderr, "volume %d: %.1f%% (%d of %d MB), %.0f MB/s\n", volumeId,
			float64(pos)*100/float64(total), pos>>20, total>>20, float64(pos)/(1<<20)/now.Sub(started).Seconds())
	}
}
