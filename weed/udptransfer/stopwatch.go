package udptransfer

import (
	"fmt"
	"os"
	"time"
)

type watch struct {
	label string
	t1    time.Time
}

func StartWatch(s string) *watch {
	return &watch{
		label: s,
		t1:    time.Now(),
	}
}

func (w *watch) StopLoops(loop int, size int) {
	tu := time.Now().Sub(w.t1).Nanoseconds()
	timePerLoop := float64(tu) / float64(loop)
	throughput := float64(loop*size) * 1e6 / float64(tu)
	tu_ms := float64(tu) / 1e6
	fmt.Fprintf(os.Stderr, "%s tu=%.2f ms tpl=%.0f ns throughput=%.2f K/s\n", w.label, tu_ms, timePerLoop, throughput)
}

var _kt = float64(1e9 / 1024)

func (w *watch) Stop(size int) {
	tu := time.Now().Sub(w.t1).Nanoseconds()
	throughput := float64(size) * _kt / float64(tu)
	tu_ms := float64(tu) / 1e6
	fmt.Fprintf(os.Stderr, "%s tu=%.2f ms throughput=%.2f K/s\n", w.label, tu_ms, throughput)
}
