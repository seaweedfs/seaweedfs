package admin

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/fs"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Assets only change with the binary; the ETag revalidates across upgrades.
const staticCacheControl = "public, max-age=3600"

type staticAsset struct {
	name   string // base name, drives Content-Type in http.ServeContent
	gz     []byte
	etag   string
	etagGz string
}

// identity returns the uncompressed bytes.
func (a *staticAsset) identity() ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(a.gz))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	return io.ReadAll(zr)
}

var staticAssets = sync.OnceValue(func() map[string]*staticAsset {
	assets := make(map[string]*staticAsset)
	err := fs.WalkDir(staticGzFS, "static_gz", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		gz, err := fs.ReadFile(staticGzFS, p)
		if err != nil {
			return err
		}
		key := strings.TrimSuffix(strings.TrimPrefix(p, "static_gz/"), ".gz")
		sum := sha256.Sum256(gz)
		hash := hex.EncodeToString(sum[:8])
		assets[key] = &staticAsset{
			name:   path.Base(key),
			gz:     gz,
			etag:   `"` + hash + `"`,
			etagGz: `"` + hash + `-gz"`,
		}
		return nil
	})
	if err != nil {
		panic("walk embedded static assets: " + err.Error())
	}
	return assets
})

// StaticHandler serves the embedded admin static assets, which are gzipped
// at generation time. Gzip-capable clients get the compressed bytes as-is;
// others get them decompressed on the fly.
func StaticHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
		asset, ok := staticAssets()[key]
		if !ok {
			http.NotFound(w, r)
			return
		}
		h := w.Header()
		h.Set("Cache-Control", staticCacheControl)
		h.Add("Vary", "Accept-Encoding")
		if acceptsGzip(r) {
			h.Set("Content-Encoding", "gzip")
			h.Set("ETag", asset.etagGz)
			// ServeContent skips Content-Length when Content-Encoding is set
			if r.Header.Get("Range") == "" {
				h.Set("Content-Length", strconv.Itoa(len(asset.gz)))
			}
			http.ServeContent(w, r, asset.name, time.Time{}, bytes.NewReader(asset.gz))
			return
		}
		data, err := asset.identity()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		h.Set("ETag", asset.etag)
		http.ServeContent(w, r, asset.name, time.Time{}, bytes.NewReader(data))
	})
}

func acceptsGzip(r *http.Request) bool {
	for _, part := range strings.Split(r.Header.Get("Accept-Encoding"), ",") {
		token, attr, hasAttr := strings.Cut(strings.TrimSpace(part), ";")
		if strings.TrimSpace(token) != "gzip" {
			continue
		}
		if !hasAttr {
			return true
		}
		q, ok := strings.CutPrefix(strings.TrimSpace(attr), "q=")
		if !ok {
			return true
		}
		v, err := strconv.ParseFloat(q, 64)
		return err == nil && v > 0
	}
	return false
}
