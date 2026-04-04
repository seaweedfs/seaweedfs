package blockcmd

import engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"

// CommandRecorder records one executed command on the host side.
type CommandRecorder func(volumeID, name string)

// CoreEventEmitter feeds one runtime observation back into the core.
type CoreEventEmitter func(ev engine.Event)

// ProjectionCacheWriter stores the latest published projection in a host-local
// cache owned by the server adapter.
type ProjectionCacheWriter interface {
	StoreProjection(volumeID string, projection engine.PublicationProjection)
}

type hostEffects struct {
	recordCommand CommandRecorder
	emitCoreEvent CoreEventEmitter
	projection    ProjectionReader
	cache         ProjectionCacheWriter
}

// NewHostEffects keeps host-side command completion effects in the server
// adapter layer while exposing only the narrow Dispatcher-facing surface.
func NewHostEffects(
	recordCommand CommandRecorder,
	emitCoreEvent CoreEventEmitter,
	projection ProjectionReader,
	cache ProjectionCacheWriter,
) HostEffects {
	return &hostEffects{
		recordCommand: recordCommand,
		emitCoreEvent: emitCoreEvent,
		projection:    projection,
		cache:         cache,
	}
}

func (effects *hostEffects) RecordCommand(volumeID, name string) {
	if effects == nil || effects.recordCommand == nil {
		return
	}
	effects.recordCommand(volumeID, name)
}

func (effects *hostEffects) EmitCoreEvent(ev engine.Event) {
	if effects == nil || effects.emitCoreEvent == nil {
		return
	}
	effects.emitCoreEvent(ev)
}

func (effects *hostEffects) PublishProjection(volumeID string, projection engine.PublicationProjection) error {
	if effects == nil || effects.cache == nil {
		return nil
	}
	proj := projection
	if effects.projection != nil {
		if latest, ok := effects.projection.Projection(volumeID); ok {
			proj = latest
		}
	}
	effects.cache.StoreProjection(volumeID, proj)
	return nil
}
