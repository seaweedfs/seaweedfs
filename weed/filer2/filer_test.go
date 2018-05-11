package filer2

import (
	"testing"
	"path/filepath"
	"fmt"
)

func TestRecursion(t *testing.T) {
	fullPath := "/home/chris/some/file/abc.jpg"
	expected := []string{
		"/", "home",
		"/home", "chris",
		"/home/chris", "some",
		"/home/chris/some", "file",
	}

	dir, _ := filepath.Split(fullPath)

	i := 0

	recursivelyEnsureDirectory(dir, func(parent, name string) error {
		if parent != expected[i] || name != expected[i+1] {
			t.Errorf("recursive directory is wrong! parent=%s dirName=%s", parent, name)
		}
		fmt.Printf("processing folder %s \t parent=%s\n", name, parent)
		i += 2
		return nil
	})
}
