package filer_pb

import "context"

type omitChunksKey struct{}

// WithChunksOmitted marks a listing as wanting attributes only. A listing over
// gRPC then asks the filer to leave the chunk lists out, and a listing served
// from a local store skips building them. The file size is carried in the
// attributes either way.
//
// Only a caller that reads attributes and nothing else may use this. In
// particular a listing that populates a cache, or one whose entries can be
// written back or deleted, needs the chunks.
func WithChunksOmitted(ctx context.Context) context.Context {
	return context.WithValue(ctx, omitChunksKey{}, true)
}

// ChunksOmitted reports whether the listing wants attributes only.
func ChunksOmitted(ctx context.Context) bool {
	return ctx.Value(omitChunksKey{}) != nil
}
