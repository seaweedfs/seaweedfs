package azure

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	. "github.com/Azure/azure-storage-blob-go/azblob"
	"io"
	"sync"
)

// copied from https://github.com/Azure/azure-storage-blob-go/blob/master/azblob/highlevel.go#L73:6
// uploadReaderAtToBlockBlob was not public

// uploadReaderAtToBlockBlob uploads a buffer in blocks to a block blob.
func uploadReaderAtToBlockBlob(ctx context.Context, reader io.ReaderAt, readerSize int64,
	blockBlobURL BlockBlobURL, o UploadToBlockBlobOptions) (CommonResponse, error) {
	if o.BlockSize == 0 {
		// If bufferSize > (BlockBlobMaxStageBlockBytes * BlockBlobMaxBlocks), then error
		if readerSize > BlockBlobMaxStageBlockBytes*BlockBlobMaxBlocks {
			return nil, errors.New("buffer is too large to upload to a block blob")
		}
		// If bufferSize <= BlockBlobMaxUploadBlobBytes, then Upload should be used with just 1 I/O request
		if readerSize <= BlockBlobMaxUploadBlobBytes {
			o.BlockSize = BlockBlobMaxUploadBlobBytes // Default if unspecified
		} else {
			o.BlockSize = readerSize / BlockBlobMaxBlocks   // buffer / max blocks = block size to use all 50,000 blocks
			if o.BlockSize < BlobDefaultDownloadBlockSize { // If the block size is smaller than 4MB, round up to 4MB
				o.BlockSize = BlobDefaultDownloadBlockSize
			}
			// StageBlock will be called with blockSize blocks and a Parallelism of (BufferSize / BlockSize).
		}
	}

	if readerSize <= BlockBlobMaxUploadBlobBytes {
		// If the size can fit in 1 Upload call, do it this way
		var body io.ReadSeeker = io.NewSectionReader(reader, 0, readerSize)
		if o.Progress != nil {
			body = pipeline.NewRequestBodyProgress(body, o.Progress)
		}
		return blockBlobURL.Upload(ctx, body, o.BlobHTTPHeaders, o.Metadata, o.AccessConditions, o.BlobAccessTier, o.BlobTagsMap, o.ClientProvidedKeyOptions, o.ImmutabilityPolicyOptions)
	}

	var numBlocks = uint16(((readerSize - 1) / o.BlockSize) + 1)

	blockIDList := make([]string, numBlocks) // Base-64 encoded block IDs
	progress := int64(0)
	progressLock := &sync.Mutex{}

	err := DoBatchTransfer(ctx, BatchTransferOptions{
		OperationName: "uploadReaderAtToBlockBlob",
		TransferSize:  readerSize,
		ChunkSize:     o.BlockSize,
		Parallelism:   o.Parallelism,
		Operation: func(offset int64, count int64, ctx context.Context) error {
			// This function is called once per block.
			// It is passed this block's offset within the buffer and its count of bytes
			// Prepare to read the proper block/section of the buffer
			var body io.ReadSeeker = io.NewSectionReader(reader, offset, count)
			blockNum := offset / o.BlockSize
			if o.Progress != nil {
				blockProgress := int64(0)
				body = pipeline.NewRequestBodyProgress(body,
					func(bytesTransferred int64) {
						diff := bytesTransferred - blockProgress
						blockProgress = bytesTransferred
						progressLock.Lock() // 1 goroutine at a time gets a progress report
						progress += diff
						o.Progress(progress)
						progressLock.Unlock()
					})
			}

			// Block IDs are unique values to avoid issue if 2+ clients are uploading blocks
			// at the same time causing PutBlockList to get a mix of blocks from all the clients.
			blockIDList[blockNum] = base64.StdEncoding.EncodeToString(newUUID().bytes())
			_, err := blockBlobURL.StageBlock(ctx, blockIDList[blockNum], body, o.AccessConditions.LeaseAccessConditions, nil, o.ClientProvidedKeyOptions)
			return err
		},
	})
	if err != nil {
		return nil, err
	}
	// All put blocks were successful, call Put Block List to finalize the blob
	return blockBlobURL.CommitBlockList(ctx, blockIDList, o.BlobHTTPHeaders, o.Metadata, o.AccessConditions, o.BlobAccessTier, o.BlobTagsMap, o.ClientProvidedKeyOptions, o.ImmutabilityPolicyOptions)
}

// The UUID reserved variants.
const (
	reservedNCS       byte = 0x80
	reservedRFC4122   byte = 0x40
	reservedMicrosoft byte = 0x20
	reservedFuture    byte = 0x00
)

type uuid [16]byte

// NewUUID returns a new uuid using RFC 4122 algorithm.
func newUUID() (u uuid) {
	u = uuid{}
	// Set all bits to randomly (or pseudo-randomly) chosen values.
	rand.Read(u[:])
	u[8] = (u[8] | reservedRFC4122) & 0x7F // u.setVariant(ReservedRFC4122)

	var version byte = 4
	u[6] = (u[6] & 0xF) | (version << 4) // u.setVersion(4)
	return
}

// String returns an unparsed version of the generated UUID sequence.
func (u uuid) String() string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:])
}

func (u uuid) bytes() []byte {
	return u[:]
}
