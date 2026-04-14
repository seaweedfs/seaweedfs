package page_writer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_PageChunkWrittenIntervalList(t *testing.T) {
	list := newChunkWrittenIntervalList()

	assert.Equal(t, 0, list.size(), "empty list")

	list.MarkWritten(0, 5, 1)
	assert.Equal(t, 1, list.size(), "one interval")

	list.MarkWritten(0, 5, 2)
	assert.Equal(t, 1, list.size(), "duplicated interval2")

	list.MarkWritten(95, 100, 3)
	assert.Equal(t, 2, list.size(), "two intervals")

	list.MarkWritten(50, 60, 4)
	assert.Equal(t, 3, list.size(), "three intervals")

	list.MarkWritten(50, 55, 5)
	assert.Equal(t, 4, list.size(), "three intervals merge")

	list.MarkWritten(40, 50, 6)
	assert.Equal(t, 5, list.size(), "three intervals grow forward")

	list.MarkWritten(50, 65, 7)
	assert.Equal(t, 4, list.size(), "three intervals grow backward")

	list.MarkWritten(70, 80, 8)
	assert.Equal(t, 5, list.size(), "four intervals")

	list.MarkWritten(60, 70, 9)
	assert.Equal(t, 6, list.size(), "three intervals merged")

	list.MarkWritten(59, 71, 10)
	assert.Equal(t, 6, list.size(), "covered three intervals")

	list.MarkWritten(5, 59, 11)
	assert.Equal(t, 5, list.size(), "covered two intervals")

	list.MarkWritten(70, 99, 12)
	assert.Equal(t, 5, list.size(), "covered one intervals")

}

type interval struct {
	start    int64
	stop     int64
	expected bool
}

func Test_PageChunkWrittenIntervalList1(t *testing.T) {
	list := newChunkWrittenIntervalList()
	inputs := []interval{
		{1, 5, true},
		{2, 3, true},
	}
	for i, input := range inputs {
		list.MarkWritten(input.start, input.stop, int64(i)+1)
		actual := hasData(list, 0, 4)
		if actual != input.expected {
			t.Errorf("input [%d,%d) expected %v actual %v", input.start, input.stop, input.expected, actual)
		}
	}
}

func hasData(usage *ChunkWrittenIntervalList, chunkStartOffset, x int64) bool {
	for t := usage.head.next; t != usage.tail; t = t.next {
		logicStart := chunkStartOffset + t.StartOffset
		logicStop := chunkStartOffset + t.stopOffset
		if logicStart <= x && x < logicStop {
			return true
		}
	}
	return false
}

func TestIsComplete_AdjacentIntervals(t *testing.T) {
	// Linux FUSE delivers writes up to FUSE_MAX_PAGES_PER_REQ
	// (typically 1 MiB) per op, so a 2 MiB chunk filled by sequential
	// writes arrives as two adjacent 1 MiB writes. addInterval does
	// not merge adjacent intervals into one, but IsComplete must
	// still report the chunk as fully covered — otherwise
	// maybeMoveToSealed never fires, the chunk stays writable
	// forever, and the write buffer cap cannot drain. Regression for
	// the TestWriteBufferCap Linux CI deadlock on PR #9066.
	const chunkSize int64 = 2 * 1024 * 1024

	cases := []struct {
		name     string
		writes   [][2]int64
		expected bool
	}{
		{
			name:     "single full write",
			writes:   [][2]int64{{0, chunkSize}},
			expected: true,
		},
		{
			name:     "two adjacent halves in order",
			writes:   [][2]int64{{0, chunkSize / 2}, {chunkSize / 2, chunkSize}},
			expected: true,
		},
		{
			name:     "two adjacent halves out of order",
			writes:   [][2]int64{{chunkSize / 2, chunkSize}, {0, chunkSize / 2}},
			expected: true,
		},
		{
			name: "eight adjacent eighths",
			writes: func() [][2]int64 {
				const n = 8
				step := chunkSize / n
				out := make([][2]int64, 0, n)
				for i := int64(0); i < n; i++ {
					out = append(out, [2]int64{i * step, (i + 1) * step})
				}
				return out
			}(),
			expected: true,
		},
		{
			name:     "gap in the middle",
			writes:   [][2]int64{{0, chunkSize / 4}, {chunkSize / 2, chunkSize}},
			expected: false,
		},
		{
			name:     "missing last byte",
			writes:   [][2]int64{{0, chunkSize - 1}},
			expected: false,
		},
		{
			name:     "missing first byte",
			writes:   [][2]int64{{1, chunkSize}},
			expected: false,
		},
		{
			name:     "overlapping intervals covering everything",
			writes:   [][2]int64{{0, chunkSize*3/4 + 1}, {chunkSize / 2, chunkSize}},
			expected: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			list := newChunkWrittenIntervalList()
			for i, w := range tc.writes {
				list.MarkWritten(w[0], w[1], int64(i+1))
			}
			assert.Equal(t, tc.expected, list.IsComplete(chunkSize), "IsComplete mismatch")
		})
	}
}
