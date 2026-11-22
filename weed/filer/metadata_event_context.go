package filer

import "context"

type suppressMetadataEventsKey struct{}

type createdParentsKey struct{}

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

// WithCreatedParents suppresses events like WithSuppressedMetadataEvents and
// collects the parent directories created, or promoted from files, on the way.
// No event of the caller covers them, so it announces them once its change
// commits.
func WithCreatedParents(ctx context.Context, created *[]*Entry) context.Context {
	return context.WithValue(WithSuppressedMetadataEvents(ctx), createdParentsKey{}, created)
}

func recordCreatedParent(ctx context.Context, dirEntry *Entry) bool {
	created, _ := ctx.Value(createdParentsKey{}).(*[]*Entry)
	if created == nil {
		return false
	}
	*created = append(*created, dirEntry)
	return true
}
