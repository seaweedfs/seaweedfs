package topology

import (
	"sync"
	"sync/atomic"
)

// growthFence keeps volume growth and assigns out of a collection being
// deleted. Growth holds lock shared while it creates one volume; a delete
// raises deleting and then takes lock, so the volume in flight has registered,
// and is found by the delete, before the delete lists the collection's servers.
// deleteLock runs one delete of the collection at a time.
type growthFence struct {
	lock       sync.RWMutex
	deleting   atomic.Int32
	deleteLock sync.Mutex
}

// FenceGrowth stops assigns and volume growth in the collection, and returns
// once the volume growth is creating has registered. The returned func lets
// them back in, for a delete that failed and leaves the collection in place.
func (c *Collection) FenceGrowth() (unfence func()) {
	c.growth.deleting.Add(1)
	c.growth.lock.Lock()
	c.growth.lock.Unlock()
	return func() {
		c.growth.deleting.Add(-1)
	}
}

// FenceGrowth fences the collection published under the name. A collection
// unpublished for being empty before its fence went up is let go, and the one
// published since, where growth registers its volumes, is fenced instead.
// It then waits for a delete already running on the collection, and finds
// nothing once that delete removed it, so a delete never reaches a collection
// recreated under the name after it was called. finish ends the delete, and
// lets growth back in unless the collection was deleted.
func (t *Topology) FenceGrowth(collectionName string) (c *Collection, finish func(deleted bool), found bool) {
	var unfence func()
	for {
		if c, found = t.FindCollection(collectionName); !found {
			return nil, nil, false
		}
		unfence = c.FenceGrowth()
		if published, stillFound := t.FindCollection(collectionName); stillFound && published == c {
			break
		}
		unfence()
	}
	c.growth.deleteLock.Lock()
	if published, stillFound := t.FindCollection(collectionName); !stillFound || published != c {
		c.growth.deleteLock.Unlock()
		unfence()
		return nil, nil, false
	}
	return c, func(deleted bool) {
		if !deleted {
			unfence()
		}
		c.growth.deleteLock.Unlock()
	}, true
}

func (c *Collection) isDeleting() bool {
	return c.growth.deleting.Load() > 0
}

func (c *Collection) unlockForGrowth() {
	c.growth.lock.RUnlock()
}

// lockForGrowth holds off a delete of the grow request's collection while
// growth creates one volume in it, and fails once a delete fenced it. A
// collection dropped only because its last layout emptied is followed to the
// one now published under its name, where the volume registers.
func (t *Topology) lockForGrowth(option *VolumeGrowOption) bool {
	for {
		c := option.collection
		c.growth.lock.RLock()
		if c.isDeleting() {
			c.growth.lock.RUnlock()
			return false
		}
		if published, found := t.FindCollection(c.Name); found && published == c {
			return true
		}
		c.growth.lock.RUnlock()
		option.collection = t.getOrCreateCollection(c.Name)
	}
}

// deleteEmptyCollection keeps a collection that growth or a delete holds
// published: the volume in flight registers by name, and must land where the
// delete lists the servers, not in a new collection under the same name.
func (t *Topology) deleteEmptyCollection(c *Collection) {
	if !c.growth.lock.TryLock() {
		return
	}
	defer c.growth.lock.Unlock()
	if !c.isDeleting() {
		t.DeleteCollection(c.Name)
	}
}

func (t *Topology) getOrCreateCollection(collectionName string) *Collection {
	return t.collectionMap.Get(collectionName, func() interface{} {
		c := NewCollection(collectionName, t.volumeSizeLimit, t.replicationAsMin)
		c.maxWritableVolumes = t.maxWritableVolumes
		return c
	}).(*Collection)
}
