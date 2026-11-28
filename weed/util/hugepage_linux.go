//go:build linux

package util

import (
	"os"
	"path/filepath"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"golang.org/x/sys/unix"
)

const (
	thpEnabledPath = "/sys/kernel/mm/transparent_hugepage/enabled"
	thpDefragPath  = "/sys/kernel/mm/transparent_hugepage/defrag"
	thpMaxPtesPath = "/sys/kernel/mm/transparent_hugepage/khugepaged/max_ptes_none"
	thpSizesGlob   = "/sys/kernel/mm/transparent_hugepage/hugepages-*kB/enabled"
)

// TransparentHugePages is the host's transparent huge page policy as the
// kernel reports it.
type TransparentHugePages struct {
	Enabled     string   // always, madvise or never
	Defrag      string   // always, defer, defer+madvise or madvise
	MaxPtesNone string   // khugepaged/max_ptes_none, empty when unreadable
	SizeModes   []string // hugepages-<size>kB/enabled, Linux 6.8+; empty when unreadable
}

func readTHPMode(path string) string {
	content, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return parseTHPMode(string(content))
}

func readTHPValue(path string) string {
	content, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return parseTHPValue(string(content))
}

func readTHPSizeModes() []string {
	paths, _ := filepath.Glob(thpSizesGlob)
	var modes []string
	for _, path := range paths {
		modes = append(modes, readTHPMode(path))
	}
	return modes
}

// ReadTransparentHugePages reports the host THP policy, or nil when the kernel
// does not expose it (non-Linux kernels and some sandboxes).
func ReadTransparentHugePages() *TransparentHugePages {
	enabled := readTHPMode(thpEnabledPath)
	if enabled == "" {
		return nil
	}
	return &TransparentHugePages{
		Enabled:     enabled,
		Defrag:      readTHPMode(thpDefragPath),
		MaxPtesNone: readTHPValue(thpMaxPtesPath),
		SizeModes:   readTHPSizeModes(),
	}
}

// processHugePagesDisabled reports whether prctl(PR_SET_THP_DISABLE), which
// a launcher such as systemd can set and children inherit, turned THP off for
// this process. Its except-advised variant still covers the unadvised heap.
// known is false when the kernel refuses to answer.
func processHugePagesDisabled() (disabled, known bool) {
	value, err := unix.PrctlRetInt(unix.PR_GET_THP_DISABLE, 0, 0, 0, 0)
	return value != 0, err == nil
}

// LogTransparentHugePages writes the host THP policy to the log once at
// startup. It deliberately changes nothing: THP is a host-wide setting that no
// process should rewrite, and GODEBUG=disablethp=1 is the only per-process
// control Go offers. Recommended host settings are in
// https://go.dev/doc/gc-guide#Linux_transparent_huge_pages
func LogTransparentHugePages() {
	status := ReadTransparentHugePages()
	if status == nil {
		return
	}
	hostAllowsHeap, hostKnown := anonHugePagesAlways(status.Enabled, status.SizeModes)
	if status.Enabled == "never" && hostKnown && !hostAllowsHeap {
		glog.V(0).Infof("transparent huge pages: disabled by the host; the Go heap will not use them")
		return
	}
	goHeapTHP := "false"
	if (hostAllowsHeap || !hostKnown) && !goHeapHugePagesDisabled(os.Getenv("GODEBUG")) {
		switch disabled, known := processHugePagesDisabled(); {
		case known && disabled:
		case !known || !hostKnown:
			goHeapTHP = "unknown"
		default:
			goHeapTHP = "true"
		}
	}
	glog.V(0).Infof("transparent huge pages: enabled=%s defrag=%s khugepaged.max_ptes_none=%s go_heap_thp=%s; see https://go.dev/doc/gc-guide#Linux_transparent_huge_pages",
		status.Enabled, status.Defrag, status.MaxPtesNone, goHeapTHP)
}
