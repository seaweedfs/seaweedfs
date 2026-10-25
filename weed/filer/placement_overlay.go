package filer

// PlacementOverlay resolves a write-time placement override for a collection —
// the disk type, replication, and data center new volumes of that collection
// should land on — or ok=false when the collection has none.
//
// It exists so a feature can steer where a bound collection's data lands without
// every operator setting an fs.configure rule by hand. The type is deliberately
// generic: it names no placement-policy concepts, so a downstream build
// registers whatever overlay it wants without coupling this seam to it.
type PlacementOverlay func(collection string) (diskType, replication, dataCenter string, ok bool)

// newPlacementOverlay builds the overlay for a filer. Enterprise sets it in an
// init() pulled in by a side-effect import, the way plugin-worker handlers
// register — the setter cannot live here because this package is overwritten by
// the sync.
var newPlacementOverlay func(*Filer) PlacementOverlay

// RegisterPlacementOverlay installs the factory. It is meant to be called from a
// package init() — before any Filer is constructed and before serving starts —
// so the unsynchronized read in NewFiler never races the write. The last
// registration wins; there is only ever one overlay.
func RegisterPlacementOverlay(factory func(*Filer) PlacementOverlay) {
	newPlacementOverlay = factory
}

// ResolvePlacement returns the overlay's placement for a collection, or
// ok=false when there is no overlay or the collection is not steered. The write
// path applies these between the explicit request value and the filer.conf
// rule, so an overlay overrides the rule but yields to an explicit request.
func (f *Filer) ResolvePlacement(collection string) (diskType, replication, dataCenter string, ok bool) {
	if f == nil || f.placementOverlay == nil || collection == "" {
		return "", "", "", false
	}
	return f.placementOverlay(collection)
}
