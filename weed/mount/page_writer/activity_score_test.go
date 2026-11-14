package page_writer

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestActivityScoreHandlesFutureTimestamp(t *testing.T) {
	for _, test := range []struct {
		name string
		call func(*ActivityScore) int64
		want int64
	}{
		{"MarkRead", func(score *ActivityScore) int64 {
			score.MarkRead()
			return score.ActivityScore()
		}, 4352},
		{"MarkWrite", func(score *ActivityScore) int64 {
			score.MarkWrite()
			return score.ActivityScore()
		}, 5120},
		{"ActivityScore", (*ActivityScore).ActivityScore, 4096},
	} {
		t.Run(test.name, func(t *testing.T) {
			score := NewActivityScore()
			atomic.StoreInt64(&score.lastActiveTsNs, time.Now().Add(time.Hour).UnixNano())
			atomic.StoreInt64(&score.decayedActivenessScore, 4096)
			defer func() {
				if caught := recover(); caught != nil {
					t.Fatalf("future last-active timestamp must not panic: %v", caught)
				}
			}()
			if got := test.call(score); got != test.want {
				t.Fatalf("score=%d, want %d without backwards-time decay", got, test.want)
			}
		})
	}
}
