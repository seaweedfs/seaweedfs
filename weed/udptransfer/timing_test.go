package udptransfer

import (
	"math"
	"testing"
	"time"
)

func Test_sleep(t *testing.T) {
	const loops = 10
	var intervals = [...]int64{1, 1e3, 1e4, 1e5, 1e6, 1e7}
	var ret [len(intervals)][loops]int64
	for j := 0; j < len(intervals); j++ {
		v := time.Duration(intervals[j])
		for i := 0; i < loops; i++ {
			t0 := NowNS()
			time.Sleep(v)
			ret[j][i] = NowNS() - t0
		}
	}
	for j := 0; j < len(intervals); j++ {
		var exp, sum, stdev float64
		exp = float64(intervals[j])
		for _, v := range ret[j] {
			sum += float64(v)
			stdev += math.Pow(float64(v)-exp, 2)
		}
		stdev /= float64(loops)
		stdev = math.Sqrt(stdev)
		t.Logf("interval=%s sleeping=%s stdev/k=%.2f", time.Duration(intervals[j]), time.Duration(sum/loops), stdev/1e3)
	}
}
