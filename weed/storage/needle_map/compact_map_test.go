package needle_map

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestSegmentBsearchKey(t *testing.T) {
	testSegment := &CompactMapSegment{
		list: []CompactNeedleValue{
			CompactNeedleValue{key: 10},
			CompactNeedleValue{key: 20},
			CompactNeedleValue{key: 21},
			CompactNeedleValue{key: 26},
			CompactNeedleValue{key: 30},
		},
		firstKey: 10,
		lastKey:  30,
	}

	testCases := []struct {
		name      string
		cs        *CompactMapSegment
		key       types.NeedleId
		wantIndex int
		wantFound bool
	}{
		{
			name:      "empty segment",
			cs:        newCompactMapSegment(0),
			key:       123,
			wantIndex: 0,
			wantFound: false,
		},
		{
			name:      "new key, insert at beggining",
			cs:        testSegment,
			key:       5,
			wantIndex: 0,
			wantFound: false,
		},
		{
			name:      "new key, insert at end",
			cs:        testSegment,
			key:       100,
			wantIndex: 5,
			wantFound: false,
		},
		{
			name:      "new key, insert second",
			cs:        testSegment,
			key:       12,
			wantIndex: 1,
			wantFound: false,
		},
		{
			name:      "new key, insert in middle",
			cs:        testSegment,
			key:       23,
			wantIndex: 3,
			wantFound: false,
		},
		{
			name:      "key #1",
			cs:        testSegment,
			key:       10,
			wantIndex: 0,
			wantFound: true,
		},
		{
			name:      "key #2",
			cs:        testSegment,
			key:       20,
			wantIndex: 1,
			wantFound: true,
		},
		{
			name:      "key #3",
			cs:        testSegment,
			key:       21,
			wantIndex: 2,
			wantFound: true,
		},
		{
			name:      "key #4",
			cs:        testSegment,
			key:       26,
			wantIndex: 3,
			wantFound: true,
		},
		{
			name:      "key #5",
			cs:        testSegment,
			key:       30,
			wantIndex: 4,
			wantFound: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			index, found := tc.cs.bsearchKey(tc.key)
			if got, want := index, tc.wantIndex; got != want {
				t.Errorf("expected %v, got %v", want, got)
			}
			if got, want := found, tc.wantFound; got != want {
				t.Errorf("expected %v, got %v", want, got)
			}
		})
	}
}

func TestSegmentSet(t *testing.T) {
	testSegment := &CompactMapSegment{
		list: []CompactNeedleValue{
			CompactNeedleValue{key: 10, offset: OffsetToCompact(types.Uint32ToOffset(0)), size: 100},
			CompactNeedleValue{key: 20, offset: OffsetToCompact(types.Uint32ToOffset(100)), size: 200},
			CompactNeedleValue{key: 30, offset: OffsetToCompact(types.Uint32ToOffset(300)), size: 300},
		},
		firstKey: 10,
		lastKey:  30,
	}

	if got, want := testSegment.len(), 3; got != want {
		t.Errorf("got starting size %d, want %d", got, want)
	}
	if got, want := testSegment.cap(), 3; got != want {
		t.Errorf("got starting capacity %d, want %d", got, want)
	}

	testSets := []struct {
		name       string
		key        types.NeedleId
		offset     types.Offset
		size       types.Size
		wantOffset types.Offset
		wantSize   types.Size
	}{
		{
			name: "insert at beggining",
			key:  5, offset: types.Uint32ToOffset(1000), size: 123,
			wantOffset: types.Uint32ToOffset(0), wantSize: 0,
		},
		{
			name: "insert at end",
			key:  51, offset: types.Uint32ToOffset(7000), size: 456,
			wantOffset: types.Uint32ToOffset(0), wantSize: 0,
		},
		{
			name: "insert in middle",
			key:  25, offset: types.Uint32ToOffset(8000), size: 789,
			wantOffset: types.Uint32ToOffset(0), wantSize: 0,
		},
		{
			name: "update existing",
			key:  30, offset: types.Uint32ToOffset(9000), size: 999,
			wantOffset: types.Uint32ToOffset(300), wantSize: 300,
		},
	}

	for _, ts := range testSets {
		offset, size := testSegment.set(ts.key, ts.offset, ts.size)
		if offset != ts.wantOffset {
			t.Errorf("%s: got offset %v, want %v", ts.name, offset, ts.wantOffset)
		}
		if size != ts.wantSize {
			t.Errorf("%s: got size %v, want %v", ts.name, size, ts.wantSize)
		}
	}

	wantSegment := &CompactMapSegment{
		list: []CompactNeedleValue{
			CompactNeedleValue{key: 5, offset: OffsetToCompact(types.Uint32ToOffset(1000)), size: 123},
			CompactNeedleValue{key: 10, offset: OffsetToCompact(types.Uint32ToOffset(0)), size: 100},
			CompactNeedleValue{key: 20, offset: OffsetToCompact(types.Uint32ToOffset(100)), size: 200},
			CompactNeedleValue{key: 25, offset: OffsetToCompact(types.Uint32ToOffset(8000)), size: 789},
			CompactNeedleValue{key: 30, offset: OffsetToCompact(types.Uint32ToOffset(9000)), size: 999},
			CompactNeedleValue{key: 51, offset: OffsetToCompact(types.Uint32ToOffset(7000)), size: 456},
		},
		firstKey: 5,
		lastKey:  51,
	}
	if !reflect.DeepEqual(testSegment, wantSegment) {
		t.Errorf("got result segment %v, want %v", testSegment, wantSegment)
	}

	if got, want := testSegment.len(), 6; got != want {
		t.Errorf("got result size %d, want %d", got, want)
	}
	if got, want := testSegment.cap(), 6; got != want {
		t.Errorf("got result capacity %d, want %d", got, want)
	}
}

func TestSegmentSetOrdering(t *testing.T) {
	keys := []types.NeedleId{}
	for i := 0; i < SegmentChunkSize; i++ {
		keys = append(keys, types.NeedleId(i))
	}

	r := rand.New(rand.NewSource(123456789))
	r.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	cs := newCompactMapSegment(0)
	for _, k := range keys {
		_, _ = cs.set(k, types.Uint32ToOffset(123), 456)
	}
	if got, want := cs.len(), SegmentChunkSize; got != want {
		t.Errorf("expected size %d, got %d", want, got)
	}
	for i := 1; i < cs.len(); i++ {
		if ka, kb := cs.list[i-1].key, cs.list[i].key; ka >= kb {
			t.Errorf("found out of order entries at (%d, %d) = (%d, %d)", i-1, i, ka, kb)
		}
	}
}

func TestSegmentGet(t *testing.T) {
	testSegment := &CompactMapSegment{
		list: []CompactNeedleValue{
			CompactNeedleValue{key: 10, offset: OffsetToCompact(types.Uint32ToOffset(0)), size: 100},
			CompactNeedleValue{key: 20, offset: OffsetToCompact(types.Uint32ToOffset(100)), size: 200},
			CompactNeedleValue{key: 30, offset: OffsetToCompact(types.Uint32ToOffset(300)), size: 300},
		},
		firstKey: 10,
		lastKey:  30,
	}

	testCases := []struct {
		name      string
		key       types.NeedleId
		wantValue *CompactNeedleValue
		wantFound bool
	}{
		{
			name:      "invalid key",
			key:       99,
			wantValue: nil,
			wantFound: false,
		},
		{
			name:      "key #1",
			key:       10,
			wantValue: &testSegment.list[0],
			wantFound: true,
		},
		{
			name:      "key #2",
			key:       20,
			wantValue: &testSegment.list[1],
			wantFound: true,
		},
		{
			name:      "key #3",
			key:       30,
			wantValue: &testSegment.list[2],
			wantFound: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			value, found := testSegment.get(tc.key)
			if got, want := value, tc.wantValue; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			if got, want := found, tc.wantFound; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
		})
	}
}

func TestSegmentDelete(t *testing.T) {
	testSegment := &CompactMapSegment{
		list: []CompactNeedleValue{
			CompactNeedleValue{key: 10, offset: OffsetToCompact(types.Uint32ToOffset(0)), size: 100},
			CompactNeedleValue{key: 20, offset: OffsetToCompact(types.Uint32ToOffset(100)), size: 200},
			CompactNeedleValue{key: 30, offset: OffsetToCompact(types.Uint32ToOffset(300)), size: 300},
			CompactNeedleValue{key: 40, offset: OffsetToCompact(types.Uint32ToOffset(600)), size: 400},
		},
		firstKey: 10,
		lastKey:  40,
	}

	testDeletes := []struct {
		name string
		key  types.NeedleId
		want types.Size
	}{
		{
			name: "invalid key",
			key:  99,
			want: 0,
		},
		{
			name: "delete key #2",
			key:  20,
			want: 200,
		},
		{
			name: "delete key #4",
			key:  40,
			want: 400,
		},
	}

	for _, td := range testDeletes {
		size := testSegment.delete(td.key)
		if got, want := size, td.want; got != want {
			t.Errorf("%s: got %v, want %v", td.name, got, want)
		}
	}

	wantSegment := &CompactMapSegment{
		list: []CompactNeedleValue{
			CompactNeedleValue{key: 10, offset: OffsetToCompact(types.Uint32ToOffset(0)), size: 100},
			CompactNeedleValue{key: 20, offset: OffsetToCompact(types.Uint32ToOffset(100)), size: -200},
			CompactNeedleValue{key: 30, offset: OffsetToCompact(types.Uint32ToOffset(300)), size: 300},
			CompactNeedleValue{key: 40, offset: OffsetToCompact(types.Uint32ToOffset(600)), size: -400},
		},
		firstKey: 10,
		lastKey:  40,
	}
	if !reflect.DeepEqual(testSegment, wantSegment) {
		t.Errorf("got result segment %v, want %v", testSegment, wantSegment)
	}
}

func TestSegmentForKey(t *testing.T) {
	testMap := NewCompactMap()

	tests := []struct {
		name string
		key  types.NeedleId
		want *CompactMapSegment
	}{
		{
			name: "first segment",
			key:  12,
			want: &CompactMapSegment{
				list:     []CompactNeedleValue{},
				chunk:    0,
				firstKey: MaxCompactKey,
				lastKey:  0,
			},
		},
		{
			name: "second segment, gapless",
			key:  SegmentChunkSize + 34,
			want: &CompactMapSegment{
				list:     []CompactNeedleValue{},
				chunk:    1,
				firstKey: MaxCompactKey,
				lastKey:  0,
			},
		},
		{
			name: "gapped segment",
			key:  (5 * SegmentChunkSize) + 56,
			want: &CompactMapSegment{
				list:     []CompactNeedleValue{},
				chunk:    5,
				firstKey: MaxCompactKey,
				lastKey:  0,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cs := testMap.segmentForKey(tc.key)
			if !reflect.DeepEqual(cs, tc.want) {
				t.Errorf("got segment %v, want %v", cs, tc.want)
			}
		})
	}

	wantMap := &CompactMap{
		segments: map[Chunk]*CompactMapSegment{
			0: &CompactMapSegment{
				list:     []CompactNeedleValue{},
				chunk:    0,
				firstKey: MaxCompactKey,
				lastKey:  0,
			},
			1: &CompactMapSegment{
				list:     []CompactNeedleValue{},
				chunk:    1,
				firstKey: MaxCompactKey,
				lastKey:  0,
			},
			5: &CompactMapSegment{
				list:     []CompactNeedleValue{},
				chunk:    5,
				firstKey: MaxCompactKey,
				lastKey:  0,
			},
		},
	}
	if !reflect.DeepEqual(testMap, wantMap) {
		t.Errorf("got map %v, want %v", testMap, wantMap)
	}
}

func TestAscendingVisit(t *testing.T) {
	cm := NewCompactMap()
	for _, nid := range []types.NeedleId{20, 7, 40000, 300000, 0, 100, 500, 10000, 200000} {
		cm.Set(nid, types.Uint32ToOffset(123), 456)
	}

	got := []NeedleValue{}
	err := cm.AscendingVisit(func(nv NeedleValue) error {
		got = append(got, nv)
		return nil
	})
	if err != nil {
		t.Errorf("got error %v, expected none", err)
	}

	want := []NeedleValue{
		NeedleValue{Key: 0, Offset: types.Uint32ToOffset(123), Size: 456},
		NeedleValue{Key: 7, Offset: types.Uint32ToOffset(123), Size: 456},
		NeedleValue{Key: 20, Offset: types.Uint32ToOffset(123), Size: 456},
		NeedleValue{Key: 100, Offset: types.Uint32ToOffset(123), Size: 456},
		NeedleValue{Key: 500, Offset: types.Uint32ToOffset(123), Size: 456},
		NeedleValue{Key: 10000, Offset: types.Uint32ToOffset(123), Size: 456},
		NeedleValue{Key: 40000, Offset: types.Uint32ToOffset(123), Size: 456},
		NeedleValue{Key: 200000, Offset: types.Uint32ToOffset(123), Size: 456},
		NeedleValue{Key: 300000, Offset: types.Uint32ToOffset(123), Size: 456},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got values %v, want %v", got, want)
	}
}

func TestRandomInsert(t *testing.T) {
	count := 8 * SegmentChunkSize
	keys := []types.NeedleId{}
	for i := 0; i < count; i++ {
		keys = append(keys, types.NeedleId(i))
	}

	r := rand.New(rand.NewSource(123456789))
	r.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	cm := NewCompactMap()
	for _, k := range keys {
		_, _ = cm.Set(k, types.Uint32ToOffset(123), 456)
	}
	if got, want := cm.Len(), count; got != want {
		t.Errorf("expected size %d, got %d", want, got)
	}

	last := -1
	err := cm.AscendingVisit(func(nv NeedleValue) error {
		key := int(nv.Key)
		if key <= last {
			return fmt.Errorf("found out of order entries (%d vs %d)", key, last)
		}
		last = key
		return nil
	})
	if err != nil {
		t.Errorf("got error %v, expected none", err)
	}

	// Given that we've written a integer multiple of SegmentChunkSize, all
	// segments should be fully utilized and capacity-adjusted.
	if l, c := cm.Len(), cm.Cap(); l != c {
		t.Errorf("map length (%d) doesn't match capacity (%d)", l, c)
	}
}
