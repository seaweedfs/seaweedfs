package filer2

import (
	"bytes"
	"fmt"
	"io"
	"math"

	"github.com/golang/protobuf/proto"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func HasChunkManifest(chunks []*filer_pb.FileChunk) bool {
	for _, chunk := range chunks {
		if chunk.IsChunkManifest {
			return true
		}
	}
	return false
}

func ResolveChunkManifest(lookupFileIdFn LookupFileIdFunctionType, chunks []*filer_pb.FileChunk) (dataChunks, manifestChunks []*filer_pb.FileChunk, manefestResolveErr error) {
	// TODO maybe parallel this
	for _, chunk := range chunks {
		if !chunk.IsChunkManifest {
			dataChunks = append(dataChunks, chunk)
			continue
		}

		// IsChunkManifest
		data, err := fetchChunk(lookupFileIdFn, chunk.FileId, chunk.CipherKey, chunk.IsCompressed)
		if err != nil {
			return chunks, nil, fmt.Errorf("fail to read manifest %s: %v", chunk.FileId, err)
		}
		m := &filer_pb.FileChunkManifest{}
		if err := proto.Unmarshal(data, m); err != nil {
			return chunks, nil, fmt.Errorf("fail to unmarshal manifest %s: %v", chunk.FileId, err)
		}
		manifestChunks = append(manifestChunks, chunk)
		// recursive
		dchunks, mchunks, subErr := ResolveChunkManifest(lookupFileIdFn, m.Chunks)
		if subErr != nil {
			return chunks, nil, subErr
		}
		dataChunks = append(dataChunks, dchunks...)
		manifestChunks = append(manifestChunks, mchunks...)
	}
	return
}

func fetchChunk(lookupFileIdFn LookupFileIdFunctionType, fileId string, cipherKey []byte, isGzipped bool) ([]byte, error) {
	urlString, err := lookupFileIdFn(fileId)
	if err != nil {
		glog.V(1).Infof("operation LookupFileId %s failed, err: %v", fileId, err)
		return nil, err
	}
	var buffer bytes.Buffer
	err = util.ReadUrlAsStream(urlString, cipherKey, isGzipped, true, 0, 0, func(data []byte) {
		buffer.Write(data)
	})
	if err != nil {
		glog.V(0).Infof("read %s failed, err: %v", fileId, err)
		return nil, err
	}

	return buffer.Bytes(), nil
}

func MaybeManifestize(saveFunc SaveDataAsChunkFunctionType, dataChunks []*filer_pb.FileChunk) (chunks []*filer_pb.FileChunk, err error) {
	return doMaybeManifestize(saveFunc, dataChunks, 10000, mergeIntoManifest)
}

func doMaybeManifestize(saveFunc SaveDataAsChunkFunctionType, inputChunks []*filer_pb.FileChunk, mergeFactor int, mergefn func(saveFunc SaveDataAsChunkFunctionType, dataChunks []*filer_pb.FileChunk) (manifestChunk *filer_pb.FileChunk, err error)) (chunks []*filer_pb.FileChunk, err error) {

	var dataChunks []*filer_pb.FileChunk
	for _, chunk := range inputChunks {
		if !chunk.IsChunkManifest {
			dataChunks = append(dataChunks, chunk)
		} else {
			chunks = append(chunks, chunk)
		}
	}

	manifestBatch := mergeFactor
	remaining := len(dataChunks)
	for i := 0; i+manifestBatch <= len(dataChunks); i += manifestBatch {
		chunk, err := mergefn(saveFunc, dataChunks[i:i+manifestBatch])
		if err != nil {
			return dataChunks, err
		}
		chunks = append(chunks, chunk)
		remaining -= manifestBatch
	}
	// remaining
	for i := len(dataChunks) - remaining; i < len(dataChunks); i++ {
		chunks = append(chunks, dataChunks[i])
	}
	return
}

func mergeIntoManifest(saveFunc SaveDataAsChunkFunctionType, dataChunks []*filer_pb.FileChunk) (manifestChunk *filer_pb.FileChunk, err error) {

	// create and serialize the manifest
	data, serErr := proto.Marshal(&filer_pb.FileChunkManifest{
		Chunks: dataChunks,
	})
	if serErr != nil {
		return nil, fmt.Errorf("serializing manifest: %v", serErr)
	}

	minOffset, maxOffset := int64(math.MaxInt64), int64(math.MinInt64)
	for k := 0; k < len(dataChunks); k++ {
		chunk := dataChunks[k]
		if minOffset > int64(chunk.Offset) {
			minOffset = chunk.Offset
		}
		if maxOffset < int64(chunk.Size)+chunk.Offset {
			maxOffset = int64(chunk.Size) + chunk.Offset
		}
	}

	manifestChunk, _, _, err = saveFunc(bytes.NewReader(data), "", 0)
	if err != nil {
		return nil, err
	}
	manifestChunk.IsChunkManifest = true
	manifestChunk.Offset = minOffset
	manifestChunk.Size = uint64(maxOffset - minOffset)

	return
}

type SaveDataAsChunkFunctionType func(reader io.Reader, name string, offset int64) (chunk *filer_pb.FileChunk, collection, replication string, err error)
