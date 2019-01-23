package storage

import (
	"fmt"
	"testing"
)

func TestMemoryStat(t *testing.T) {
	total, free, err := MemoryStat()
	if err != nil {
		t.Fail()
	}

	if total <= 0 {
		println("total", total, "free", free)
		t.Fail()
	}
}

func TestDiskStat(t *testing.T) {
	total, free, device, mountPoint, err := DiskStat("..")
	if err != nil {
		t.Fail()
	}

	if total <= 0 {
		println("total", total, "free", free)
		t.Fail()
	}

	fmt.Println("device", device, "mountPoint", mountPoint)

}

func TestAvgLoad(t *testing.T) {
	load1, load5, load15, err := LoadStat()
	if err != nil {
		t.Fail()
	}

	if load1 <= 0 {
		println("load1", load1, "load5", load5, "load15", load15)
		t.Fail()
	}
}

func TestProcessStat(t *testing.T) {
	name, cpuUsage, rss, err := ProcessStat()
	if err != nil {
		t.Fail()
	}

	if rss <= 0 {
		println("name", name, "cpuUsage", cpuUsage, "RSS", rss)
		t.Fail()
	}
}
