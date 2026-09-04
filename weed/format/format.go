// Package format maps the internal structure of container file formats onto
// storage chunk boundaries.
//
// An adapter translates one format into three things the core understands: a
// list of extent sizes, an alignment quantum, and an opaque payload. Adapters
// never see chunks, file ids, or authorization; the core never learns what an
// MPEG-TS packet or a parquet row group is.
package format

import (
	"context"
	"errors"
	"io"
	"net/url"
)

// LayoutKey is the filer Extended key holding the encoded Layout. The
// x-seaweedfs- prefix keeps it out of HTTP response headers.
const LayoutKey = "x-seaweedfs-format-layout"

// ViewParam is the query parameter selecting a format view on GET requests.
// Adapters rendering self-referential URLs must use it.
const ViewParam = "format.view"

// ErrNoSuchView reports that a view request addresses nothing servable; the
// server answers 404.
var ErrNoSuchView = errors.New("no such view")

// Layout describes how a file's structure maps to byte extents.
type Layout struct {
	Format      string  // adapter name
	ExtentSizes []int64 // extent lengths in file order; they sum to the file size
	Align       int64   // quantum for cutting inside an oversized extent; 1 cuts anywhere
	Payload     []byte  // adapter-owned metadata, opaque to the core
}

// Hint carries the cheap identification signals available to Sniff.
type Hint struct {
	Name        string
	ContentType string
	Size        int64
	Head        []byte
	Tail        []byte
}

// Format is the mandatory adapter identity. Capabilities beyond it are
// discovered by type assertion.
type Format interface {
	Name() string
}

// Sniffer cheaply recognizes the format from identification signals. Repack
// gates on it before parsing, and policy-driven detection will rely on it;
// ingest-only adapters, whose files carry their layout from birth, skip it.
type Sniffer interface {
	Sniff(h Hint) bool
}

// Indexer derives a Layout from the complete stored bytes.
type Indexer interface {
	Index(ctx context.Context, r io.ReaderAt, size int64) (*Layout, error)
}

// SidecarIndexer derives a Layout from an external index document supplied at
// ingest, before the media bytes arrive.
type SidecarIndexer interface {
	IndexSidecar(sidecar []byte) (*Layout, error)
}

// Object is everything a Viewer may know about the file it serves.
type Object struct {
	Name   string
	Size   int64
	Layout *Layout
}

// ViewRequest carries the request parameters of a ?view= request.
type ViewRequest struct {
	Query url.Values
}

// ViewPlan tells the server what to serve. The server executes it on the
// normal streaming path; adapters stay pure functions of request and layout.
type ViewPlan struct {
	ContentType string
	Body        []byte // rendered document; when nil, stream Extent instead
	Extent      int
}

// Viewer answers ?view= requests.
type Viewer interface {
	View(req ViewRequest, obj Object) (*ViewPlan, error)
}
