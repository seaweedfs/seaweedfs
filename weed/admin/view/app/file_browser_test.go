package app

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
)

func TestFileBrowserPageSizeScriptUsesSelectValue(t *testing.T) {
	data := dash.FileBrowserData{
		CurrentPath: "/",
		ParentPath:  "/",
		PageSize:    20,
	}

	var rendered bytes.Buffer
	if err := FileBrowser(data).Render(context.Background(), &rendered); err != nil {
		t.Fatalf("render file browser: %v", err)
	}

	html := rendered.String()

	if !strings.Contains(html, "document.getElementById(pageSizeSelectId)") {
		t.Fatalf("expected page size script to read from select element ID")
	}

	if strings.Contains(html, "&limit=' + this.value") {
		t.Fatalf("unexpected use of this.value in page size script")
	}

	if !strings.Contains(html, "file-browser-page-size-top") {
		t.Fatalf("expected top page size selector ID to be present")
	}

	if !strings.Contains(html, "file-browser-page-size-bottom") {
		t.Fatalf("expected bottom page size selector ID to be present")
	}
}
