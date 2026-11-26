package mount

import (
	"bytes"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// A small file the filer keeps inside its entry (-saveToFilerLimit, or
// ?saveInside=true on the upload) has its bytes in the entry's content and no
// chunks. What a mount writes becomes chunks, so the content has to become
// one as well before the first change of the file's data: dropped instead,
// the bytes a write left alone read as zeros from then on. A change of the
// file's size cuts the content, or makes it a chunk for the bytes after it to
// read as zeros.

// inlineContent returns the bytes of entry's content that are the file's: a
// mount from before this may have cut the file's size and left the content.
func inlineContent(entry *filer_pb.Entry) []byte {
	content := entry.GetContent()
	if size := entry.GetAttributes().GetFileSize(); uint64(len(content)) > size {
		content = content[:size]
	}
	return content
}

// bufferInlineContent turns the content of the handle's entry into a write of
// the handle's own, older than one at tsNs, and takes it out of the entry.
// The flush stores it as a chunk with what else the handle wrote. The caller
// holds the handle's lock for writing.
func (fh *FileHandle) bufferInlineContent(entry *filer_pb.Entry, tsNs int64) error {
	if len(entry.GetContent()) == 0 {
		return nil
	}
	if content := inlineContent(entry); len(content) > 0 {
		buffered, err := fh.dirtyPages.AddPage(0, content, true, tsNs-1)
		if err == nil && buffered < int64(len(content)) {
			err = fmt.Errorf("buffered %d of %d bytes", buffered, len(content))
		}
		if err != nil {
			return fmt.Errorf("inline content of %s: %w", fh.FullPath(), err)
		}
		fh.pageWrites.Add(1)
	}
	entry.Content = nil
	fh.dirtyMetadata, fh.dirtyContent = true, true
	return nil
}

// resizeInlineContent gets entry's content ready for the file's size to
// become size: cut to it, or, when the file grows past the content, stored
// with save in chunks of chunkSize as buffered writes are, for the bytes after
// it to read as zeros. chunked reports that entry has chunks more.
func resizeInlineContent(entry *filer_pb.Entry, size uint64, tsNs int64, chunkSize int64, save filer.SaveDataAsChunkFunctionType) (chunked bool, err error) {
	if len(entry.GetContent()) == 0 {
		return false, nil
	}
	content := inlineContent(entry)
	if size <= uint64(len(content)) {
		if entry.Content = content[:size]; size == 0 {
			entry.Content = nil
		}
		return false, nil
	}
	var chunks []*filer_pb.FileChunk
	for offset := int64(0); offset < int64(len(content)); {
		n := int64(len(content)) - offset
		if chunkSize > 0 && n > chunkSize {
			n = chunkSize
		}
		chunk, err := save(bytes.NewReader(content[offset:offset+n]), entry.GetName(), offset, tsNs, uint64(n))
		if err != nil {
			return false, fmt.Errorf("inline content of %s: %w", entry.GetName(), err)
		}
		chunks = append(chunks, chunk)
		offset += n
	}
	entry.Chunks = append(entry.Chunks, chunks...)
	entry.Content = nil
	return len(chunks) > 0, nil
}
