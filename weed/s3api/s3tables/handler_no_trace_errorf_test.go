package s3tables

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// glog.Errorf with a constant message and no format verb is a trace, not an
// error report: every CreateNamespace call logged
// "S3Tables: handleCreateNamespace called" at ERROR.
func TestNoTraceLoggedAtErrorLevel(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("no sources found (%v)", err) // positive control: the scan must see the package
	}
	re := regexp.MustCompile(`glog\.Errorf\("([^"]*)"\)`)
	scanned := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		scanned++
		for _, m := range re.FindAllStringSubmatch(string(b), -1) {
			if !strings.Contains(m[1], "%") {
				t.Errorf("%s: glog.Errorf(%q) logs a constant trace at ERROR; use glog.V(n).Infof", f, m[1])
			}
		}
	}
	if scanned < 5 {
		t.Fatalf("scanned only %d source files", scanned)
	}
}
