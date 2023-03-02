package filer

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

type IntervalInt int

func (i IntervalInt) SetStartStop(start, stop int64) {
}
func (i IntervalInt) Clone() IntervalValue {
	return i
}

func TestIntervalList_Overlay(t *testing.T) {
	list := NewIntervalList[IntervalInt]()
	list.Overlay(0, 100, 1, 1)
	list.Overlay(50, 150, 2, 2)
	list.Overlay(200, 250, 3, 3)
	list.Overlay(225, 250, 4, 4)
	list.Overlay(175, 210, 5, 5)
	list.Overlay(0, 25, 6, 6)
	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 6, list.Len())
	println()
	list.Overlay(50, 150, 7, 7)
	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 6, list.Len())
}

func TestIntervalList_Overlay2(t *testing.T) {
	list := NewIntervalList[IntervalInt]()
	list.Overlay(50, 100, 1, 1)
	list.Overlay(0, 50, 2, 2)
	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
}

func TestIntervalList_Overlay3(t *testing.T) {
	list := NewIntervalList[IntervalInt]()
	list.Overlay(50, 100, 1, 1)
	assert.Equal(t, 1, list.Len())

	list.Overlay(0, 60, 2, 2)
	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 2, list.Len())
}

func TestIntervalList_Overlay4(t *testing.T) {
	list := NewIntervalList[IntervalInt]()
	list.Overlay(50, 100, 1, 1)
	list.Overlay(0, 100, 2, 2)
	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 1, list.Len())
}

func TestIntervalList_Overlay5(t *testing.T) {
	list := NewIntervalList[IntervalInt]()
	list.Overlay(50, 100, 1, 1)
	list.Overlay(0, 110, 2, 2)
	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 1, list.Len())
}

func TestIntervalList_Overlay6(t *testing.T) {
	list := NewIntervalList[IntervalInt]()
	list.Overlay(50, 100, 1, 1)
	list.Overlay(50, 110, 2, 2)
	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 1, list.Len())
}

func TestIntervalList_Overlay7(t *testing.T) {
	list := NewIntervalList[IntervalInt]()
	list.Overlay(50, 100, 1, 1)
	list.Overlay(50, 90, 2, 2)
	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 2, list.Len())
}

func TestIntervalList_Overlay8(t *testing.T) {
	list := NewIntervalList[IntervalInt]()
	list.Overlay(50, 100, 1, 1)
	list.Overlay(60, 90, 2, 2)
	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 3, list.Len())
}

func TestIntervalList_Overlay9(t *testing.T) {
	list := NewIntervalList[IntervalInt]()
	list.Overlay(50, 100, 1, 1)
	list.Overlay(60, 100, 2, 2)
	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 2, list.Len())
}

func TestIntervalList_Overlay10(t *testing.T) {
	list := NewIntervalList[IntervalInt]()
	list.Overlay(50, 100, 1, 1)
	list.Overlay(60, 110, 2, 2)
	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 2, list.Len())
}

func TestIntervalList_Overlay11(t *testing.T) {
	list := NewIntervalList[IntervalInt]()
	list.Overlay(0, 100, 1, 1)
	list.Overlay(100, 110, 2, 2)
	list.Overlay(0, 90, 3, 3)
	list.Overlay(0, 80, 4, 4)
	list.Overlay(0, 90, 5, 5)
	list.Overlay(90, 90, 6, 6)
	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 3, list.Len())
}

func TestIntervalList_insertInterval1(t *testing.T) {
	list := NewIntervalList[IntervalInt]()
	list.InsertInterval(50, 150, 2, 2)
	list.InsertInterval(200, 250, 3, 3)

	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 2, list.Len())
}

func TestIntervalList_insertInterval2(t *testing.T) {
	list := NewIntervalList[IntervalInt]()
	list.InsertInterval(50, 150, 2, 2)
	list.InsertInterval(0, 25, 3, 3)

	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 2, list.Len())
}

func TestIntervalList_insertInterval3(t *testing.T) {
	list := NewIntervalList[IntervalInt]()
	list.InsertInterval(50, 150, 2, 2)
	list.InsertInterval(200, 250, 4, 4)

	list.InsertInterval(0, 75, 3, 3)

	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 3, list.Len())
}

func TestIntervalList_insertInterval4(t *testing.T) {
	list := NewIntervalList[IntervalInt]()
	list.InsertInterval(200, 250, 4, 4)

	list.InsertInterval(0, 225, 3, 3)

	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 2, list.Len())
}

func TestIntervalList_insertInterval5(t *testing.T) {
	list := NewIntervalList[IntervalInt]()
	list.InsertInterval(200, 250, 4, 4)

	list.InsertInterval(0, 225, 5, 5)

	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 2, list.Len())
}

func TestIntervalList_insertInterval6(t *testing.T) {
	list := NewIntervalList[IntervalInt]()

	list.InsertInterval(50, 150, 2, 2)
	list.InsertInterval(200, 250, 4, 4)

	list.InsertInterval(0, 275, 1, 1)

	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 5, list.Len())
}

func TestIntervalList_insertInterval7(t *testing.T) {
	list := NewIntervalList[IntervalInt]()

	list.InsertInterval(50, 150, 2, 2)
	list.InsertInterval(200, 250, 4, 4)

	list.InsertInterval(75, 275, 1, 1)

	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 4, list.Len())
}

func TestIntervalList_insertInterval8(t *testing.T) {
	list := NewIntervalList[IntervalInt]()

	list.InsertInterval(50, 150, 2, 2)
	list.InsertInterval(200, 250, 4, 4)

	list.InsertInterval(75, 275, 3, 3)

	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 4, list.Len())
}

func TestIntervalList_insertInterval9(t *testing.T) {
	list := NewIntervalList[IntervalInt]()

	list.InsertInterval(50, 150, 2, 2)
	list.InsertInterval(200, 250, 4, 4)

	list.InsertInterval(50, 150, 3, 3)

	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 2, list.Len())
}

func TestIntervalList_insertInterval10(t *testing.T) {
	list := NewIntervalList[IntervalInt]()

	list.InsertInterval(50, 100, 2, 2)

	list.InsertInterval(200, 300, 4, 4)

	list.InsertInterval(100, 200, 5, 5)

	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 3, list.Len())
}

func TestIntervalList_insertInterval11(t *testing.T) {
	list := NewIntervalList[IntervalInt]()

	list.InsertInterval(0, 64, 1, 1)

	list.InsertInterval(72, 136, 3, 3)

	list.InsertInterval(64, 128, 2, 2)

	list.InsertInterval(68, 72, 4, 4)

	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 4, list.Len())
}

type IntervalStruct struct {
	x     int
	start int64
	stop  int64
}

func newIntervalStruct(i int) IntervalStruct {
	return IntervalStruct{
		x: i,
	}
}

func (i IntervalStruct) SetStartStop(start, stop int64) {
	i.start, i.stop = start, stop
}
func (i IntervalStruct) Clone() IntervalValue {
	return &IntervalStruct{
		x:     i.x,
		start: i.start,
		stop:  i.stop,
	}
}

func TestIntervalList_insertIntervalStruct(t *testing.T) {
	list := NewIntervalList[IntervalStruct]()

	list.InsertInterval(0, 64, 1, newIntervalStruct(1))

	list.InsertInterval(64, 72, 2, newIntervalStruct(2))

	list.InsertInterval(72, 136, 3, newIntervalStruct(3))

	list.InsertInterval(64, 68, 4, newIntervalStruct(4))

	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 4, list.Len())
}
