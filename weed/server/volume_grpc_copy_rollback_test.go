package weed_server

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLinkReplacedVolumeFilesPreservesPresentFiles(t *testing.T) {
	dir := t.TempDir()
	dat := filepath.Join(dir, "vol_1.dat")
	idx := filepath.Join(dir, "vol_1.idx")
	for _, f := range []string{dat, idx} {
		if err := os.WriteFile(f, []byte("x"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	links, complete := linkReplacedVolumeFiles(dat, idx, filepath.Join(dir, "vol_1.vif"))
	if !complete {
		t.Fatal("linking existing files should report complete")
	}
	if len(links) != 2 {
		t.Fatalf("expected 2 links for 2 present files, got %d", len(links))
	}
	for _, link := range links {
		if _, err := os.Stat(link); err != nil {
			t.Fatalf("link %s missing: %v", link, err)
		}
	}
}

func TestLinkReplacedVolumeFilesReportsUnpreservedFile(t *testing.T) {
	dir := t.TempDir()
	dat := filepath.Join(dir, "vol_1.dat")
	if err := os.WriteFile(dat, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	// a copying/ path that exists as a file cannot hold the backup links
	if err := os.WriteFile(filepath.Join(dir, "copying"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	links, complete := linkReplacedVolumeFiles(dat)
	if complete {
		t.Fatal("a present file that cannot be linked must report incomplete")
	}
	if len(links) != 0 {
		t.Fatalf("expected no links, got %v", links)
	}
}

func TestRestoreReplacedVolumeFilesPutsLinksBack(t *testing.T) {
	dir := t.TempDir()
	dat := filepath.Join(dir, "vol_1.dat")
	if err := os.WriteFile(dat, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	links, complete := linkReplacedVolumeFiles(dat)
	if !complete || len(links) != 1 {
		t.Fatalf("link setup failed: links=%v complete=%v", links, complete)
	}
	if err := os.Remove(dat); err != nil {
		t.Fatal(err)
	}

	restoreReplacedVolumeFiles(links)
	if _, err := os.Stat(dat); err != nil {
		t.Fatalf("restored file %s missing: %v", dat, err)
	}
	if _, err := os.Stat(links[0]); !os.IsNotExist(err) {
		t.Fatalf("link %s should be consumed by the restore", links[0])
	}
}

func TestRestoreReplacedVolumeFilesRestoresRepeatedPathOnce(t *testing.T) {
	dir := t.TempDir()
	idx := filepath.Join(dir, "vol_1.idx")
	if err := os.WriteFile(idx, []byte("replica"), 0644); err != nil {
		t.Fatal(err)
	}

	links, complete := linkReplacedVolumeFiles(idx, idx)
	if !complete || len(links) != 1 {
		t.Fatalf("a repeated path should link once: links=%v complete=%v", links, complete)
	}
	if err := os.Remove(idx); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(idx, []byte("copy"), 0644); err != nil {
		t.Fatal(err)
	}

	restoreReplacedVolumeFiles(links)
	got, err := os.ReadFile(idx)
	if err != nil {
		t.Fatalf("restored file %s missing: %v", idx, err)
	}
	if string(got) != "replica" {
		t.Fatalf("the second pass deleted the restored file: %q", got)
	}
}

func TestRestoreReplacedVolumeFilesReplacesSurvivingPartial(t *testing.T) {
	dir := t.TempDir()
	dat := filepath.Join(dir, "vol_1.dat")
	if err := os.WriteFile(dat, []byte("replica"), 0644); err != nil {
		t.Fatal(err)
	}
	links, complete := linkReplacedVolumeFiles(dat)
	if !complete || len(links) != 1 {
		t.Fatalf("link setup failed: links=%v complete=%v", links, complete)
	}
	if err := os.Remove(dat); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dat, []byte("copy"), 0644); err != nil {
		t.Fatal(err)
	}

	restoreReplacedVolumeFiles(links)
	got, err := os.ReadFile(dat)
	if err != nil {
		t.Fatalf("restored file %s missing: %v", dat, err)
	}
	if string(got) != "replica" {
		t.Fatalf("a surviving partial replaced the restored file: %q", got)
	}
	if _, err := os.Stat(links[0]); !os.IsNotExist(err) {
		t.Fatalf("link %s should be consumed by the restore", links[0])
	}
}
