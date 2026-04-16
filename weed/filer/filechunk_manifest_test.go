package filer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestDoMaybeManifestize(t *testing.T) {
	var manifestTests = []struct {
		inputs   []*filer_pb.FileChunk
		expected []*filer_pb.FileChunk
	}{
		{
			inputs: []*filer_pb.FileChunk{
				{FileId: "1", IsChunkManifest: false},
				{FileId: "2", IsChunkManifest: false},
				{FileId: "3", IsChunkManifest: false},
				{FileId: "4", IsChunkManifest: false},
			},
			expected: []*filer_pb.FileChunk{
				{FileId: "12", IsChunkManifest: true},
				{FileId: "34", IsChunkManifest: true},
			},
		},
		{
			inputs: []*filer_pb.FileChunk{
				{FileId: "1", IsChunkManifest: true},
				{FileId: "2", IsChunkManifest: false},
				{FileId: "3", IsChunkManifest: false},
				{FileId: "4", IsChunkManifest: false},
			},
			expected: []*filer_pb.FileChunk{
				{FileId: "1", IsChunkManifest: true},
				{FileId: "23", IsChunkManifest: true},
				{FileId: "4", IsChunkManifest: false},
			},
		},
		{
			inputs: []*filer_pb.FileChunk{
				{FileId: "1", IsChunkManifest: false},
				{FileId: "2", IsChunkManifest: true},
				{FileId: "3", IsChunkManifest: false},
				{FileId: "4", IsChunkManifest: false},
			},
			expected: []*filer_pb.FileChunk{
				{FileId: "2", IsChunkManifest: true},
				{FileId: "13", IsChunkManifest: true},
				{FileId: "4", IsChunkManifest: false},
			},
		},
		{
			inputs: []*filer_pb.FileChunk{
				{FileId: "1", IsChunkManifest: true},
				{FileId: "2", IsChunkManifest: true},
				{FileId: "3", IsChunkManifest: false},
				{FileId: "4", IsChunkManifest: false},
			},
			expected: []*filer_pb.FileChunk{
				{FileId: "1", IsChunkManifest: true},
				{FileId: "2", IsChunkManifest: true},
				{FileId: "34", IsChunkManifest: true},
			},
		},
	}

	for i, mtest := range manifestTests {
		println("test", i)
		actual, _ := doMaybeManifestize(nil, mtest.inputs, 2, mockMerge)
		assertEqualChunks(t, mtest.expected, actual)
	}

}

func assertEqualChunks(t *testing.T, expected, actual []*filer_pb.FileChunk) {
	assert.Equal(t, len(expected), len(actual))
	for i := 0; i < len(actual); i++ {
		assertEqualChunk(t, actual[i], expected[i])
	}
}
func assertEqualChunk(t *testing.T, expected, actual *filer_pb.FileChunk) {
	assert.Equal(t, expected.FileId, actual.FileId)
	assert.Equal(t, expected.IsChunkManifest, actual.IsChunkManifest)
}

func mockMerge(saveFunc SaveDataAsChunkFunctionType, dataChunks []*filer_pb.FileChunk) (manifestChunk *filer_pb.FileChunk, err error) {

	var buf bytes.Buffer
	minOffset, maxOffset := int64(math.MaxInt64), int64(math.MinInt64)
	for k := 0; k < len(dataChunks); k++ {
		chunk := dataChunks[k]
		buf.WriteString(chunk.FileId)
		if minOffset > int64(chunk.Offset) {
			minOffset = chunk.Offset
		}
		if maxOffset < int64(chunk.Size)+chunk.Offset {
			maxOffset = int64(chunk.Size) + chunk.Offset
		}
	}

	manifestChunk = &filer_pb.FileChunk{
		FileId: buf.String(),
	}
	manifestChunk.IsChunkManifest = true
	manifestChunk.Offset = minOffset
	manifestChunk.Size = uint64(maxOffset - minOffset)

	return
}

// ---------------------------------------------------------------------------
// In-memory manifest store for round-trip tests
// ---------------------------------------------------------------------------

// testManifestStore provides in-memory save/resolve for manifest tests
// without requiring volume servers.
type testManifestStore struct {
	stored map[string][]byte
	nextID int
}

func newTestManifestStore() *testManifestStore {
	return &testManifestStore{stored: make(map[string][]byte)}
}

func (s *testManifestStore) saveFunc() SaveDataAsChunkFunctionType {
	return func(reader io.Reader, name string, offset int64, tsNs int64) (*filer_pb.FileChunk, error) {
		data, err := io.ReadAll(reader)
		if err != nil {
			return nil, err
		}
		s.nextID++
		chunk := &filer_pb.FileChunk{
			Fid: &filer_pb.FileId{VolumeId: 999, FileKey: uint64(s.nextID), Cookie: 0},
		}
		s.stored[chunk.GetFileIdString()] = data
		return chunk, nil
	}
}

// resolve expands a single manifest chunk by deserializing the stored proto.
func (s *testManifestStore) resolve(chunk *filer_pb.FileChunk) ([]*filer_pb.FileChunk, error) {
	data, ok := s.stored[chunk.GetFileIdString()]
	if !ok {
		return nil, fmt.Errorf("manifest %q not in store", chunk.GetFileIdString())
	}
	m := &filer_pb.FileChunkManifest{}
	if err := proto.Unmarshal(data, m); err != nil {
		return nil, err
	}
	filer_pb.AfterEntryDeserialization(m.Chunks)
	return m.Chunks, nil
}

// resolveAll expands all manifest chunks, returns (dataChunks, manifestChunks).
func (s *testManifestStore) resolveAll(chunks []*filer_pb.FileChunk) (data, manifests []*filer_pb.FileChunk, err error) {
	for _, c := range chunks {
		if !c.IsChunkManifest {
			data = append(data, c)
			continue
		}
		manifests = append(manifests, c)
		sub, resolveErr := s.resolve(c)
		if resolveErr != nil {
			return nil, nil, resolveErr
		}
		data = append(data, sub...)
	}
	return
}

func testChunk(volId uint32, key uint64, cookie uint32, offset int64, size uint64, tsNs int64) *filer_pb.FileChunk {
	return &filer_pb.FileChunk{
		Fid:          &filer_pb.FileId{VolumeId: volId, FileKey: key, Cookie: cookie},
		Offset:       offset,
		Size:         size,
		ModifiedTsNs: tsNs,
	}
}

// ---------------------------------------------------------------------------
// Manifest round-trip: create -> serialize -> deserialize -> verify
// ---------------------------------------------------------------------------

func TestManifestRoundTripPreservesChunks(t *testing.T) {
	store := newTestManifestStore()

	chunks := []*filer_pb.FileChunk{
		testChunk(1, 1, 100, 0, 1000, 10),
		testChunk(1, 2, 200, 1000, 1000, 20),
		testChunk(2, 3, 300, 2000, 1000, 30),
	}

	origIds := make([]string, len(chunks))
	for i, c := range chunks {
		origIds[i] = c.GetFileIdString()
	}

	// Use real mergeIntoManifest with batch=3 so all go into one manifest
	result, err := doMaybeManifestize(store.saveFunc(), chunks, 3, mergeIntoManifest)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 || !result[0].IsChunkManifest {
		t.Fatalf("expected 1 manifest chunk, got %d chunks", len(result))
	}
	if result[0].Offset != 0 || result[0].Size != 3000 {
		t.Errorf("manifest coverage: offset=%d size=%d, want 0/3000", result[0].Offset, result[0].Size)
	}

	resolved, err := store.resolve(result[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(resolved) != 3 {
		t.Fatalf("expected 3 sub-chunks, got %d", len(resolved))
	}
	for i, got := range resolved {
		if got.GetFileIdString() != origIds[i] {
			t.Errorf("chunk %d: fileId %q != %q", i, got.GetFileIdString(), origIds[i])
		}
		if got.Offset != chunks[i].Offset {
			t.Errorf("chunk %d: offset %d != %d", i, got.Offset, chunks[i].Offset)
		}
		if got.Size != chunks[i].Size {
			t.Errorf("chunk %d: size %d != %d", i, got.Size, chunks[i].Size)
		}
		if got.ModifiedTsNs != chunks[i].ModifiedTsNs {
			t.Errorf("chunk %d: ts %d != %d", i, got.ModifiedTsNs, chunks[i].ModifiedTsNs)
		}
	}
}

// ---------------------------------------------------------------------------
// Compact resolved overlapping manifests: older sub-chunks become garbage
// ---------------------------------------------------------------------------

func TestCompactResolvedOverlappingManifests(t *testing.T) {
	store := newTestManifestStore()

	// Batch 1: older writes covering [0, 3000)
	batch1 := []*filer_pb.FileChunk{
		testChunk(1, 1, 1, 0, 1000, 1),
		testChunk(1, 2, 2, 1000, 1000, 2),
		testChunk(1, 3, 3, 2000, 1000, 3),
	}
	origBatch1Ids := make(map[string]bool)
	for _, c := range batch1 {
		origBatch1Ids[c.GetFileIdString()] = true
	}

	// Batch 2: newer writes covering [0, 3000)
	batch2 := []*filer_pb.FileChunk{
		testChunk(2, 1, 1, 0, 1000, 10),
		testChunk(2, 2, 2, 1000, 1000, 11),
		testChunk(2, 3, 3, 2000, 1000, 12),
	}
	origBatch2Ids := make(map[string]bool)
	for _, c := range batch2 {
		origBatch2Ids[c.GetFileIdString()] = true
	}

	// Manifestize each batch separately (simulates successive flush cycles)
	save := store.saveFunc()
	manifest1, err := doMaybeManifestize(save, batch1, 3, mergeIntoManifest)
	if err != nil {
		t.Fatal(err)
	}
	manifest2, err := doMaybeManifestize(save, batch2, 3, mergeIntoManifest)
	if err != nil {
		t.Fatal(err)
	}

	// Resolve all manifests into sub-chunks
	allChunks := append(append([]*filer_pb.FileChunk{}, manifest1...), manifest2...)
	dataChunks, _, err := store.resolveAll(allChunks)
	if err != nil {
		t.Fatal(err)
	}
	if len(dataChunks) != 6 {
		t.Fatalf("expected 6 resolved data chunks, got %d", len(dataChunks))
	}

	// CompactFileChunks on resolved sub-chunks (nil lookupFn is fine for non-manifest chunks)
	compacted, garbage := CompactFileChunks(context.Background(), nil, dataChunks)

	if len(compacted) != 3 {
		t.Fatalf("expected 3 compacted chunks, got %d", len(compacted))
	}
	if len(garbage) != 3 {
		t.Fatalf("expected 3 garbage chunks, got %d", len(garbage))
	}

	for _, c := range compacted {
		if !origBatch2Ids[c.GetFileIdString()] {
			t.Errorf("compacted chunk %q should be from newer batch", c.GetFileIdString())
		}
	}
	for _, c := range garbage {
		if !origBatch1Ids[c.GetFileIdString()] {
			t.Errorf("garbage chunk %q should be from older batch", c.GetFileIdString())
		}
	}
}

// ---------------------------------------------------------------------------
// DoMinusChunks: old manifest sub-chunks identified as garbage vs new chunks
// ---------------------------------------------------------------------------

func TestDoMinusChunksWithResolvedManifests(t *testing.T) {
	store := newTestManifestStore()

	// Old entry: data packed into a manifest
	oldData := []*filer_pb.FileChunk{
		testChunk(1, 1, 1, 0, 1000, 1),
		testChunk(1, 2, 2, 1000, 1000, 2),
		testChunk(1, 3, 3, 2000, 1000, 3),
	}
	oldManifest, err := doMaybeManifestize(store.saveFunc(), oldData, 3, mergeIntoManifest)
	if err != nil {
		t.Fatal(err)
	}

	// Resolve old manifests into sub-chunks (what MinusChunks does internally)
	oldResolved, oldMeta, err := store.resolveAll(oldManifest)
	if err != nil {
		t.Fatal(err)
	}

	// New entry: clean re-uploaded chunks (from merge) with different file IDs
	newData := []*filer_pb.FileChunk{
		testChunk(3, 10, 10, 0, 1500, 100),
		testChunk(3, 11, 11, 1500, 1500, 101),
	}

	// DoMinusChunks: data sub-chunks in old but not in new
	deltaData := DoMinusChunks(oldResolved, newData)
	if len(deltaData) != 3 {
		t.Fatalf("expected 3 old data chunks as garbage, got %d", len(deltaData))
	}

	// DoMinusChunks: manifest chunks in old but not in new
	deltaMeta := DoMinusChunks(oldMeta, nil)
	if len(deltaMeta) != 1 {
		t.Fatalf("expected 1 old manifest chunk as garbage, got %d", len(deltaMeta))
	}
}

// ---------------------------------------------------------------------------
// Small-batch manifestize: verify sub-chunks and remainder handling
// ---------------------------------------------------------------------------

func TestManifestizeSmallBatchWithRemainder(t *testing.T) {
	store := newTestManifestStore()

	// 7 chunks with batch=3: should produce 2 manifests + 1 remainder
	chunks := make([]*filer_pb.FileChunk, 7)
	for i := range chunks {
		chunks[i] = testChunk(1, uint64(i+1), uint32(i+1), int64(i*100), 100, int64(i+1))
	}

	result, err := doMaybeManifestize(store.saveFunc(), chunks, 3, mergeIntoManifest)
	if err != nil {
		t.Fatal(err)
	}

	manifests := 0
	regular := 0
	for _, c := range result {
		if c.IsChunkManifest {
			manifests++
		} else {
			regular++
		}
	}
	if manifests != 2 {
		t.Errorf("expected 2 manifests, got %d", manifests)
	}
	if regular != 1 {
		t.Errorf("expected 1 remainder chunk, got %d", regular)
	}

	// Resolve all and verify we get back 7 data chunks
	data, _, err := store.resolveAll(result)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 7 {
		t.Fatalf("expected 7 data chunks after resolve, got %d", len(data))
	}
}

// ---------------------------------------------------------------------------
// Multiple overlapping manifests: compaction identifies all redundant sub-chunks
// ---------------------------------------------------------------------------

func TestCompactMultipleOverlappingManifestGenerations(t *testing.T) {
	store := newTestManifestStore()
	save := store.saveFunc()

	const fileSize int64 = 3000
	const chunkSize uint64 = 1000

	// Simulate 5 generations of full-file writes, each manifestized separately.
	var allManifests []*filer_pb.FileChunk
	for gen := 0; gen < 5; gen++ {
		tsBase := int64((gen + 1) * 100)
		batch := []*filer_pb.FileChunk{
			testChunk(uint32(gen+1), 1, uint32(gen), 0, chunkSize, tsBase),
			testChunk(uint32(gen+1), 2, uint32(gen), int64(chunkSize), chunkSize, tsBase+1),
			testChunk(uint32(gen+1), 3, uint32(gen), int64(chunkSize*2), chunkSize, tsBase+2),
		}
		manifest, err := doMaybeManifestize(save, batch, 3, mergeIntoManifest)
		if err != nil {
			t.Fatal(err)
		}
		allManifests = append(allManifests, manifest...)
	}

	if len(allManifests) != 5 {
		t.Fatalf("expected 5 manifest chunks, got %d", len(allManifests))
	}

	// Resolve all 5 manifests: 15 data chunks total
	dataChunks, _, err := store.resolveAll(allManifests)
	if err != nil {
		t.Fatal(err)
	}
	if len(dataChunks) != 15 {
		t.Fatalf("expected 15 resolved chunks, got %d", len(dataChunks))
	}

	// Compact: only generation 5 (the newest 3 chunks) should survive
	compacted, garbage := CompactFileChunks(context.Background(), nil, dataChunks)
	if len(compacted) != 3 {
		t.Errorf("expected 3 compacted (gen 5), got %d", len(compacted))
	}
	if len(garbage) != 12 {
		t.Errorf("expected 12 garbage (gens 1-4), got %d", len(garbage))
	}

	// Verify the surviving chunks are the newest (highest timestamps)
	for _, c := range compacted {
		if c.ModifiedTsNs < 500 {
			t.Errorf("compacted chunk ts=%d should be from gen 5 (ts >= 500)", c.ModifiedTsNs)
		}
	}
}

// ---------------------------------------------------------------------------
// Bloat detection: manifest sizes must count toward total
// ---------------------------------------------------------------------------

func TestManifestBloatDetection(t *testing.T) {
	// Simulates what shouldMergeChunks sees after multiple flush cycles.
	// With the fix, manifest sizes count toward totalChunkSize, so bloat
	// from accumulated manifests is detected.

	const fileSize uint64 = 10000

	// 1 regular chunk covering the file (from latest compaction)
	regular := []*filer_pb.FileChunk{
		{Offset: 0, Size: fileSize, FileId: "latest", ModifiedTsNs: 100,
			Fid: &filer_pb.FileId{VolumeId: 1, FileKey: 1}},
	}

	// N manifests each covering the full file (from prior flush cycles)
	for n := 1; n <= 10; n++ {
		var manifests []*filer_pb.FileChunk
		for i := 0; i < n; i++ {
			manifests = append(manifests, &filer_pb.FileChunk{
				Offset: 0, Size: fileSize, IsChunkManifest: true,
				Fid: &filer_pb.FileId{VolumeId: 900, FileKey: uint64(i + 1)},
			})
		}

		allChunks := append(append([]*filer_pb.FileChunk{}, regular...), manifests...)
		var totalStored uint64
		for _, c := range allChunks {
			totalStored += c.Size
		}
		totalFile := TotalSize(allChunks)

		expectMerge := totalStored > 2*totalFile
		t.Logf("n=%d manifests: totalStored=%d fileSize=%d ratio=%.1fx expectMerge=%v",
			n, totalStored, totalFile, float64(totalStored)/float64(totalFile), expectMerge)

		// With 1 regular (10000) + N manifests (N*10000):
		// total = (N+1)*10000, fileSize=10000, ratio = N+1
		// Merge when N+1 > 2, i.e., N >= 2
		if n >= 2 && !expectMerge {
			t.Errorf("n=%d: should trigger merge at ratio %.1f", n, float64(totalStored)/float64(totalFile))
		}
		if n < 2 && expectMerge {
			t.Errorf("n=%d: should NOT trigger merge at ratio %.1f", n, float64(totalStored)/float64(totalFile))
		}
	}
}
