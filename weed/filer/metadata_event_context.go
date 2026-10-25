package filer

import "context"

type suppressMetadataEventsKey struct{}

// WithSuppressedMetadataEvents disables automatic metadata event emission for
// nested filer operations that are part of a larger logical change, such as a
// rename implemented via create+delete.
func WithSuppressedMetadataEvents(ctx context.Context) context.Context {
	return context.WithValue(ctx, suppressMetadataEventsKey{}, true)
}

func metadataEventsSuppressed(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	suppressed, _ := ctx.Value(suppressMetadataEventsKey{}).(bool)
	return suppressed
}
