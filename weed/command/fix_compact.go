package command

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	fixCompact        = cmdFix.Flag.Bool("compact", false, "implies -verify: while verifying, write a new .dat/.idx (.cpd/.cpx) without the damaged regions and with an index that matches it exactly, then swap them in. One read of the .dat, one write of the surviving bytes; needs free space for the new .dat. Garbage left by overwrites and deletes is kept (run a vacuum afterwards). A volume without damage is left untouched; damage only at the tail is just cut off in place.")
	fixDeleteOriginal = cmdFix.Flag.Bool("deleteOriginal", false, "with -compact: delete the original .dat (kept as .dat.bak by default) once the new one is in place")
	fixCompactionMBps = cmdFix.Flag.Int("compactionMBps", 0, "with -compact: limit the write speed of the new .dat in MB/s, 0 means unlimited")
)

// datCompactor turns a verification scan into a compaction: it writes the
// index for every verified needle as it goes and, from the first damaged
// region on, the needle bytes as well. A volume without damage therefore
// costs no .dat write at all, and a damaged tail no copy either.
//
// The result is an append-ordered .idx with one entry per record in the new
// .dat (old versions and tombstones included), which is what the volume
// server writes itself and what a full scrub expects.
type datCompactor struct {
	datName, idxName string
	cpdName, cpxName string

	src        *os.File
	srcSize    int64
	superBlock super_block.SuperBlock

	cpx  *os.File
	cpxW *bufio.Writer
	cpd  backend.BackendStorageFile
	cpdW *bufio.Writer

	throttle *util.WriteThrottler

	started bool  // the .cpd is being written
	removed int64 // damaged bytes skipped so far, i.e. old offset - new offset
	outEnd  int64 // bytes of the new .dat written so far
}

type compactMode int

const (
	compactNothing   compactMode = iota // no damage: nothing was written
	compactTailOnly                     // only the tail is damaged: truncate in place, keep the new .cpx
	compactRewritten                    // a new .dat was written
)

// throttledWriter slows the writes down to the -compactionMBps limit.
type throttledWriter struct {
	w io.Writer
	t *util.WriteThrottler
}

func (tw throttledWriter) Write(p []byte) (int, error) {
	n, err := tw.w.Write(p)
	tw.t.MaybeSlowdown(int64(n))
	return n, err
}

func newDatCompactor(datName, idxName string) (*datCompactor, error) {
	c := &datCompactor{
		datName: datName, idxName: idxName,
		cpdName:  strings.TrimSuffix(datName, ".dat") + ".cpd",
		cpxName:  strings.TrimSuffix(idxName, ".idx") + ".cpx",
		throttle: util.NewWriteThrottler(int64(*fixCompactionMBps) << 20),
	}
	var err error
	if c.src, err = os.Open(datName); err != nil {
		return nil, err
	}
	st, err := c.src.Stat()
	if err != nil {
		c.src.Close()
		return nil, err
	}
	c.srcSize = st.Size()
	// the DiskFile wrapper is only used to read the super block; c.src stays ours
	if c.superBlock, err = super_block.ReadSuperBlock(backend.NewDiskFile(c.src)); err != nil {
		c.src.Close()
		return nil, err
	}
	if c.cpx, err = os.OpenFile(c.cpxName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
		c.src.Close()
		return nil, err
	}
	c.cpxW = bufio.NewWriterSize(c.cpx, 1<<20)
	// a leftover from an earlier run must not be mistaken for this run's output
	os.Remove(c.cpdName)
	return c, nil
}

func (c *datCompactor) needle(n *needle.Needle, off int64, hdr, body []byte) error {
	size := n.Size
	if !size.IsValid() {
		size = types.TombstoneFileSize
	}
	if c.started && off-c.removed != c.outEnd {
		return fmt.Errorf("internal error: needle at %d would land at %d, but the new .dat is %d bytes long", off, off-c.removed, c.outEnd)
	}
	if _, err := c.cpxW.Write(needle_map.ToBytes(n.Id, types.ToOffset(off-c.removed), size)); err != nil {
		return err
	}
	if c.started {
		if _, err := c.cpdW.Write(hdr); err != nil {
			return err
		}
		if _, err := c.cpdW.Write(body); err != nil {
			return err
		}
		c.outEnd += int64(len(hdr) + len(body))
	}
	return nil
}

func (c *datCompactor) damage(start, end int64, tail bool) error {
	if tail {
		return nil // never copied; finish() decides between truncating and the rewritten .dat
	}
	if !c.started {
		if err := c.startCopy(start, end); err != nil {
			return err
		}
	}
	c.removed += end - start
	return nil
}

// startCopy is called at the first damaged region in the middle of the file:
// everything before it is a verified, unchanged prefix, copied as one block.
func (c *datCompactor) startCopy(prefixEnd, gapEnd int64) error {
	need := c.srcSize - (gapEnd - prefixEnd) // upper bound: later gaps and a bad tail are not known yet
	if free := stats_collect.NewDiskStatus(filepath.Dir(c.cpdName)).GetFree(); free != 0 && free < uint64(need) {
		return fmt.Errorf("not enough free space in %s for the new .dat: need up to %d bytes, have %d (nothing was written)", filepath.Dir(c.cpdName), need, free)
	}
	var err error
	// preallocation keeps the size, so a shorter result needs no cleanup beyond the final truncate
	if c.cpd, err = backend.CreateVolumeFile(c.cpdName, need, 0); err != nil {
		return err
	}
	fmt.Printf("first damage at offset %d: copying the %d byte intact prefix, then streaming the rest\n", prefixEnd, prefixEnd)

	// WriterAt -> Writer; hiding any ReadFrom keeps io.CopyBuffer on its large buffer
	dst := throttledWriter{w: io.NewOffsetWriter(c.cpd, 0), t: c.throttle}
	if _, err = io.CopyBuffer(dst, io.NewSectionReader(c.src, 0, prefixEnd), make([]byte, 8<<20)); err != nil {
		return fmt.Errorf("copy prefix: %w", err)
	}
	// like a vacuum, bump the compaction revision: offsets in this .dat changed
	sb := c.superBlock
	sb.CompactionRevision++
	if _, err = c.cpd.WriteAt(sb.Bytes(), 0); err != nil {
		return fmt.Errorf("write super block: %w", err)
	}
	// continue right after the prefix; the bufio batches the small needle writes
	c.cpdW = bufio.NewWriterSize(throttledWriter{w: io.NewOffsetWriter(c.cpd, prefixEnd), t: c.throttle}, 8<<20)
	c.outEnd = prefixEnd
	c.started = true
	return nil
}

func syncAndClose(w *bufio.Writer, f *os.File) error {
	if f == nil {
		return nil
	}
	err := w.Flush()
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

func (c *datCompactor) finish(sum *datVerifySummary) (compactMode, error) {
	c.src.Close()
	if err := syncAndClose(c.cpxW, c.cpx); err != nil {
		return compactNothing, fmt.Errorf("write %s: %w", c.cpxName, err)
	}
	if !c.started {
		if sum.BadRegions == 0 {
			os.Remove(c.cpxName)
			return compactNothing, nil
		}
		return compactTailOnly, nil
	}
	err := c.cpdW.Flush()
	if err == nil {
		err = c.cpd.Truncate(c.outEnd) // drop whatever the preallocation reserved beyond the data
	}
	if cerr := c.cpd.Close(); err == nil { // DiskFile.Close syncs first
		err = cerr
	}
	c.cpd = nil
	if err != nil {
		return compactNothing, fmt.Errorf("write %s: %w", c.cpdName, err)
	}
	if want := sum.CleanEnd - c.removed; c.outEnd != want {
		return compactNothing, fmt.Errorf("internal error: new .dat is %d bytes, expected %d", c.outEnd, want)
	}
	return compactRewritten, nil
}

// abort drops everything written so far; the original files were never touched.
func (c *datCompactor) abort() {
	c.src.Close()
	if c.cpx != nil {
		c.cpx.Close()
	}
	if c.cpd != nil {
		c.cpd.Close()
	}
	os.Remove(c.cpxName)
	os.Remove(c.cpdName)
}

// backupName returns path.bak, or path.bak.<unix time> when that already exists.
func backupName(path string) string {
	bak := path + ".bak"
	if _, err := os.Stat(bak); err == nil {
		bak = fmt.Sprintf("%s.bak.%d", path, time.Now().Unix())
	}
	return bak
}

// commit swaps the new files in. The old .idx is always kept as .idx.bak, the
// old .dat as .dat.bak unless deleteOriginal is set. A failing rename is rolled
// back so the volume is never left half-swapped. The directory is synced before
// the first rename, so the new files' entries are on disk, and after the last,
// so the swap itself survives a crash.
func (c *datCompactor) commit(rewritten, deleteOriginal bool) (idxBak, datBak string, err error) {
	dir := filepath.Dir(c.datName)
	if err = util.FsyncDir(dir); err != nil {
		return
	}
	var undo []func()
	rename := func(from, to string) error {
		if err := os.Rename(from, to); err != nil {
			return err
		}
		undo = append(undo, func() { os.Rename(to, from) })
		return nil
	}
	defer func() {
		if err != nil {
			for i := len(undo) - 1; i >= 0; i-- {
				undo[i]()
			}
			util.FsyncDir(dir)
		}
	}()

	if _, statErr := os.Stat(c.idxName); statErr == nil {
		idxBak = backupName(c.idxName)
		if err = rename(c.idxName, idxBak); err != nil {
			return
		}
	}
	if rewritten {
		datBak = backupName(c.datName)
		if err = rename(c.datName, datBak); err != nil {
			return
		}
		if err = rename(c.cpdName, c.datName); err != nil {
			return
		}
	}
	if err = rename(c.cpxName, c.idxName); err != nil {
		return
	}
	// the swap is decided; a failing sync is reported but must not undo it
	if serr := util.FsyncDir(dir); serr != nil {
		fmt.Fprintf(os.Stderr, "warning: the files are swapped, but syncing the directory failed: %v\n", serr)
	}
	// Defensive: the pending-commit guard already refuses to start while a .cpc
	// marker exists, but a marker must never outlive a completed swap and make
	// the volume server roll anything forward later.
	cpc := strings.TrimSuffix(c.datName, ".dat") + ".cpc"
	if util.FileExists(cpc) {
		if rerr := os.Remove(cpc); rerr != nil {
			fmt.Fprintf(os.Stderr, "warning: could not remove the stale commit marker %s: %v\n", cpc, rerr)
		} else {
			util.FsyncDir(dir)
		}
	}
	if rewritten && deleteOriginal {
		if rerr := os.Remove(datBak); rerr != nil {
			fmt.Fprintf(os.Stderr, "could not delete the original %s: %v\n", datBak, rerr)
		} else {
			datBak = ""
			util.FsyncDir(dir)
		}
	}
	return
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
