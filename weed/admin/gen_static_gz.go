//go:build ignore

// Regenerates static_gz/, the gzipped mirror of static/ that is embedded
// into the binary. Run after changing anything under static/:
//
//	go generate ./weed/admin
package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if err := os.RemoveAll("static_gz"); err != nil {
		return err
	}
	return filepath.WalkDir("static", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		var buf bytes.Buffer
		zw, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
		if err != nil {
			return err
		}
		if _, err := zw.Write(data); err != nil {
			return err
		}
		if err := zw.Close(); err != nil {
			return err
		}
		rel, err := filepath.Rel("static", p)
		if err != nil {
			return err
		}
		out := filepath.Join("static_gz", rel+".gz")
		if err := os.MkdirAll(filepath.Dir(out), 0755); err != nil {
			return err
		}
		return os.WriteFile(out, buf.Bytes(), 0644)
	})
}
