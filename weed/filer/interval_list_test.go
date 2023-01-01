package filer

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIntervalList_Overlay(t *testing.T) {
	list := NewIntervalList[int]()
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
	list := NewIntervalList[int]()
	list.Overlay(50, 100, 1, 1)
	list.Overlay(0, 50, 2, 2)
	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
}

func TestIntervalList_Overlay3(t *testing.T) {
	list := NewIntervalList[int]()
	list.Overlay(50, 100, 1, 1)
	assert.Equal(t, 1, list.Len())

	list.Overlay(0, 60, 2, 2)
	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 2, list.Len())
}

func TestIntervalList_Overlay4(t *testing.T) {
	list := NewIntervalList[int]()
	list.Overlay(50, 100, 1, 1)
	list.Overlay(0, 100, 2, 2)
	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 1, list.Len())
}

func TestIntervalList_Overlay5(t *testing.T) {
	list := NewIntervalList[int]()
	list.Overlay(50, 100, 1, 1)
	list.Overlay(0, 110, 2, 2)
	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 1, list.Len())
}

func TestIntervalList_Overlay6(t *testing.T) {
	list := NewIntervalList[int]()
	list.Overlay(50, 100, 1, 1)
	list.Overlay(50, 110, 2, 2)
	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 1, list.Len())
}

func TestIntervalList_Overlay7(t *testing.T) {
	list := NewIntervalList[int]()
	list.Overlay(50, 100, 1, 1)
	list.Overlay(50, 90, 2, 2)
	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 2, list.Len())
}

func TestIntervalList_Overlay8(t *testing.T) {
	list := NewIntervalList[int]()
	list.Overlay(50, 100, 1, 1)
	list.Overlay(60, 90, 2, 2)
	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 3, list.Len())
}

func TestIntervalList_Overlay9(t *testing.T) {
	list := NewIntervalList[int]()
	list.Overlay(50, 100, 1, 1)
	list.Overlay(60, 100, 2, 2)
	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 2, list.Len())
}

func TestIntervalList_Overlay10(t *testing.T) {
	list := NewIntervalList[int]()
	list.Overlay(50, 100, 1, 1)
	list.Overlay(60, 110, 2, 2)
	for p := list.Front(); p != nil; p = p.Next {
		fmt.Printf("[%d,%d) %d %d\n", p.StartOffset, p.StopOffset, p.TsNs, p.Value)
	}
	assert.Equal(t, 2, list.Len())
}
