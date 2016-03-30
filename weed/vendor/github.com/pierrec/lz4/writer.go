package lz4

import (
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"runtime"
	"sync"
)

// Writer implements the LZ4 frame encoder.
type Writer struct {
	Header
	dst      io.Writer
	checksum hash.Hash32    // frame checksum
	wg       sync.WaitGroup // decompressing go routine wait group
	data     []byte         // data to be compressed, only used when dealing with block dependency as we need 64Kb to work with
	window   []byte         // last 64KB of decompressed data (block dependency) + blockMaxSize buffer
}

// NewWriter returns a new LZ4 frame encoder.
// No access to the underlying io.Writer is performed.
// The supplied Header is checked at the first Write.
// It is ok to change it before the first Write but then not until a Reset() is performed.
func NewWriter(dst io.Writer) *Writer {
	return &Writer{
		dst:      dst,
		checksum: hashPool.Get(),
		Header: Header{
			BlockMaxSize: 4 << 20,
		},
	}
}

// writeHeader builds and writes the header (magic+header) to the underlying io.Writer.
func (z *Writer) writeHeader() error {
	// Default to 4Mb if BlockMaxSize is not set
	if z.Header.BlockMaxSize == 0 {
		z.Header.BlockMaxSize = 4 << 20
	}
	// the only option that need to be validated
	bSize, ok := bsMapValue[z.Header.BlockMaxSize]
	if !ok {
		return fmt.Errorf("lz4: invalid block max size: %d", z.Header.BlockMaxSize)
	}

	// magic number(4) + header(flags(2)+[Size(8)+DictID(4)]+checksum(1)) does not exceed 19 bytes
	// Size and DictID are optional
	var buf [19]byte

	// set the fixed size data: magic number, block max size and flags
	binary.LittleEndian.PutUint32(buf[0:], frameMagic)
	flg := byte(Version << 6)
	if !z.Header.BlockDependency {
		flg |= 1 << 5
	}
	if z.Header.BlockChecksum {
		flg |= 1 << 4
	}
	if z.Header.Size > 0 {
		flg |= 1 << 3
	}
	if !z.Header.NoChecksum {
		flg |= 1 << 2
	}
	// 	if z.Header.Dict {
	// 		flg |= 1
	// 	}
	buf[4] = flg
	buf[5] = bSize << 4

	// current buffer size: magic(4) + flags(1) + block max size (1)
	n := 6
	// optional items
	if z.Header.Size > 0 {
		binary.LittleEndian.PutUint64(buf[n:], z.Header.Size)
		n += 8
	}
	// 	if z.Header.Dict {
	// 		binary.LittleEndian.PutUint32(buf[n:], z.Header.DictID)
	// 		n += 4
	// 	}

	// header checksum includes the flags, block max size and optional Size and DictID
	z.checksum.Write(buf[4:n])
	buf[n] = byte(z.checksum.Sum32() >> 8 & 0xFF)
	z.checksum.Reset()

	// header ready, write it out
	if _, err := z.dst.Write(buf[0 : n+1]); err != nil {
		return err
	}
	z.Header.done = true

	return nil
}

// Write compresses data from the supplied buffer into the underlying io.Writer.
// Write does not return until the data has been written.
//
// If the input buffer is large enough (typically in multiples of BlockMaxSize)
// the data will be compressed concurrently.
//
// Write never buffers any data unless in BlockDependency mode where it may
// do so until it has 64Kb of data, after which it never buffers any.
func (z *Writer) Write(buf []byte) (n int, err error) {
	if !z.Header.done {
		if err = z.writeHeader(); err != nil {
			return
		}
	}

	if len(buf) == 0 {
		return
	}

	if !z.NoChecksum {
		z.wg.Add(1)
		go func(b []byte) {
			z.checksum.Write(b)
			z.wg.Done()
		}(buf)
	}

	// with block dependency, require at least 64Kb of data to work with
	// not having 64Kb only matters initially to setup the first window
	if z.BlockDependency && len(z.window) == 0 {
		z.data = append(z.data, buf...)
		if len(z.data) < winSize {
			z.wg.Wait()
			return len(buf), nil
		}
		buf = z.data
		z.data = nil
	}

	// Break up the input buffer into BlockMaxSize blocks, provisioning the left over block.
	// Then compress into each of them concurrently if possible (no dependency).
	wbuf := buf
	zn := len(wbuf) / z.BlockMaxSize
	zblocks := make([]block, zn, zn+1)
	for zi := 0; zi < zn; zi++ {
		zb := &zblocks[zi]
		if z.BlockDependency {
			if zi == 0 {
				// first block does not have the window
				zb.data = append(z.window, wbuf[:z.BlockMaxSize]...)
				zb.offset = len(z.window)
				wbuf = wbuf[z.BlockMaxSize-winSize:]
			} else {
				// set the uncompressed data including the window from previous block
				zb.data = wbuf[:z.BlockMaxSize+winSize]
				zb.offset = winSize
				wbuf = wbuf[z.BlockMaxSize:]
			}
		} else {
			zb.data = wbuf[:z.BlockMaxSize]
			wbuf = wbuf[z.BlockMaxSize:]
		}

		z.wg.Add(1)
		go z.compressBlock(zb)
	}

	// left over
	if len(buf)%z.BlockMaxSize > 0 {
		zblocks = append(zblocks, block{data: wbuf})
		zb := &zblocks[zn]
		if z.BlockDependency {
			if zn == 0 {
				zb.data = append(z.window, zb.data...)
				zb.offset = len(z.window)
			} else {
				zb.offset = winSize
			}
		}
		z.wg.Add(1)
		go z.compressBlock(zb)
	}
	z.wg.Wait()

	// outputs the compressed data
	for zi, zb := range zblocks {
		_, err = z.writeBlock(&zb)

		n += len(zb.data)
		// remove the window in zb.data
		if z.BlockDependency {
			if zi == 0 {
				n -= len(z.window)
			} else {
				n -= winSize
			}
		}
		if err != nil {
			return
		}
	}

	if z.BlockDependency {
		if len(z.window) == 0 {
			z.window = make([]byte, winSize)
		}
		// last buffer may be shorter than the window
		if len(buf) > winSize {
			copy(z.window, buf[len(buf)-winSize:])
		}
	}

	return
}

// compressBlock compresses a block.
func (z *Writer) compressBlock(zb *block) {
	// compressed block size cannot exceed the input's
	zbuf := make([]byte, len(zb.data)-zb.offset)
	var (
		n   int
		err error
	)
	if z.HighCompression {
		n, err = CompressBlockHC(zb.data, zbuf, zb.offset)
	} else {
		n, err = CompressBlock(zb.data, zbuf, zb.offset)
	}

	// compressible and compressed size smaller than decompressed: ok!
	if err == nil && n > 0 && len(zb.zdata) < len(zb.data) {
		zb.compressed = true
		zb.zdata = zbuf[:n]
	} else {
		zb.zdata = zb.data[zb.offset:]
	}

	if z.BlockChecksum {
		xxh := hashPool.Get()
		xxh.Write(zb.zdata)
		zb.checksum = xxh.Sum32()
		hashPool.Put(xxh)
	}

	z.wg.Done()
}

// writeBlock writes a frame block to the underlying io.Writer (size, data).
func (z *Writer) writeBlock(zb *block) (int, error) {
	bLen := uint32(len(zb.zdata))
	if !zb.compressed {
		bLen |= 1 << 31
	}

	n := 0
	if err := binary.Write(z.dst, binary.LittleEndian, bLen); err != nil {
		return n, err
	}
	n += 4

	m, err := z.dst.Write(zb.zdata)
	n += m
	if err != nil {
		return n, err
	}

	if z.BlockChecksum {
		if err := binary.Write(z.dst, binary.LittleEndian, zb.checksum); err != nil {
			return n, err
		}
		n += 4
	}

	return n, nil
}

// Flush flushes any pending compressed data to the underlying writer.
// Flush does not return until the data has been written.
// If the underlying writer returns an error, Flush returns that error.
//
// Flush is only required when in BlockDependency mode and the total of
// data written is less than 64Kb.
func (z *Writer) Flush() error {
	if len(z.data) == 0 {
		return nil
	}
	zb := block{data: z.data}
	z.wg.Add(1)
	z.compressBlock(&zb)
	if _, err := z.writeBlock(&zb); err != nil {
		return err
	}
	return nil
}

// Close closes the Writer, flushing any unwritten data to the underlying io.Writer, but does not close the underlying io.Writer.
func (z *Writer) Close() error {
	if !z.Header.done {
		if err := z.writeHeader(); err != nil {
			return err
		}
	}

	// buffered data for the block dependency window
	if z.BlockDependency && len(z.data) > 0 {
		zb := block{data: z.data}
		z.wg.Add(1)
		z.compressBlock(&zb)
		if _, err := z.writeBlock(&zb); err != nil {
			return err
		}
	}

	if err := binary.Write(z.dst, binary.LittleEndian, uint32(0)); err != nil {
		return err
	}
	if !z.NoChecksum {
		if err := binary.Write(z.dst, binary.LittleEndian, z.checksum.Sum32()); err != nil {
			return err
		}
	}
	return nil
}

// Reset clears the state of the Writer z such that it is equivalent to its
// initial state from NewWriter, but instead writing to w.
// No access to the underlying io.Writer is performed.
func (z *Writer) Reset(w io.Writer) {
	z.Header = Header{}
	z.dst = w
	z.checksum.Reset()
	z.data = nil
	z.window = nil
}

// ReadFrom compresses the data read from the io.Reader and writes it to the underlying io.Writer.
// Returns the number of bytes read.
// It does not close the Writer.
func (z *Writer) ReadFrom(r io.Reader) (n int64, err error) {
	cpus := runtime.GOMAXPROCS(0)
	buf := make([]byte, cpus*z.BlockMaxSize)
	for {
		m, er := io.ReadFull(r, buf)
		n += int64(m)
		if er == nil || er == io.ErrUnexpectedEOF || er == io.EOF {
			if _, err = z.Write(buf[:m]); err != nil {
				return
			}
			if er == nil {
				continue
			}
			return
		}
		return n, er
	}
}
