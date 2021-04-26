package buffered_writer

import (
	"bytes"
	"io"
)

var _ = io.WriteCloser(&BufferedWriteCloser{})

type BufferedWriteCloser struct {
	buffer          bytes.Buffer
	bufferLimit     int
	position        int64
	nextFlushOffset int64
	FlushFunc       func([]byte, int64) error
	CloseFunc       func() error
}

func NewBufferedWriteCloser(bufferLimit int) *BufferedWriteCloser {
	return &BufferedWriteCloser{
		bufferLimit: bufferLimit,
	}
}

func (b *BufferedWriteCloser) Write(p []byte) (n int, err error) {

	if b.buffer.Len()+len(p) >= b.bufferLimit {
		if err := b.FlushFunc(b.buffer.Bytes(), b.nextFlushOffset); err != nil {
			return 0, err
		}
		b.nextFlushOffset += int64(b.buffer.Len())
		b.buffer.Reset()
	}

	return b.buffer.Write(p)

}

func (b *BufferedWriteCloser) Close() error {
	if b.buffer.Len() > 0 {
		if err := b.FlushFunc(b.buffer.Bytes(), b.nextFlushOffset); err != nil {
			return err
		}
	}
	if b.CloseFunc != nil {
		if err := b.CloseFunc(); err != nil {
			return err
		}
	}

	return nil
}
