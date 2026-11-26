//go:build linux

package util

import (
	"os"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"golang.org/x/sys/unix"
)

const (
	thpEnabledPath = "/sys/kernel/mm/transparent_hugepage/enabled"
	thpDefragPath  = "/sys/kernel/mm/transparent_hugepage/defrag"
	thpMaxPtesPath = "/sys/kernel/mm/transparent_hugepage/khugepaged/max_ptes_none"
)

// TransparentHugePages is the host's transparent huge page policy as the
// kernel reports it.
type TransparentHugePages struct {
	Enabled     string // always, madvise or never
	Defrag      string // always, defer, defer+madvise or madvise
	MaxPtesNone string // khugepaged/max_ptes_none, empty when unreadable
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
	}
}

// goHeapHugePagesDisabled reports whether GODEBUG=disablethp=1 is in effect,
// which makes the runtime map the heap with MADV_NOHUGEPAGE instead.
func goHeapHugePagesDisabled() bool {
	for _, setting := range strings.Split(os.Getenv("GODEBUG"), ",") {
		if strings.TrimSpace(setting) == "disablethp=1" {
			return true
		}
	}
	return false
}

// processHugePagesDisabled reports whether prctl(PR_SET_THP_DISABLE), which
// a launcher such as systemd can set and children inherit, turned THP off for
// this process. Its except-advised variant still covers the unadvised heap.
func processHugePagesDisabled() bool {
	disabled, err := unix.PrctlRetInt(unix.PR_GET_THP_DISABLE, 0, 0, 0, 0)
	return err == nil && disabled != 0
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
	if status.Enabled == "never" {
		glog.V(0).Infof("transparent huge pages: disabled by the host; the Go heap will not use them")
		return
	}
	// The runtime advises only GC metadata with MADV_HUGEPAGE, so the heap
	// itself gets huge pages only under "always".
	goHeapTHP := status.Enabled == "always" && !goHeapHugePagesDisabled() && !processHugePagesDisabled()
	glog.V(0).Infof("transparent huge pages: enabled=%s defrag=%s khugepaged.max_ptes_none=%s go_heap_thp=%t; see https://go.dev/doc/gc-guide#Linux_transparent_huge_pages",
		status.Enabled, status.Defrag, status.MaxPtesNone, goHeapTHP)
}
