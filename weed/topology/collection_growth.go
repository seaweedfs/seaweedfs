package topology

import "sync"

// growthFence keeps volume growth out of a collection being deleted. Growth
// holds lock shared while it creates one volume; a delete takes it exclusively
// to raise the fence, so the volume in flight has registered, and is found by
// the delete, before the delete lists the collection's servers.
type growthFence struct {
	lock     sync.RWMutex
	deleting int
}

// FenceGrowth stops volume growth into the collection once the volume growth
// is creating has registered. The returned func lets growth back in, for a
// delete that failed and leaves the collection in place.
func (c *Collection) FenceGrowth() (unfence func()) {
	c.growth.lock.Lock()
	c.growth.deleting++
	c.growth.lock.Unlock()
	return func() {
		c.growth.lock.Lock()
		c.growth.deleting--
		c.growth.lock.Unlock()
	}
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
		if c.growth.deleting > 0 {
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

func (t *Topology) getOrCreateCollection(collectionName string) *Collection {
	return t.collectionMap.Get(collectionName, func() interface{} {
		c := NewCollection(collectionName, t.volumeSizeLimit, t.replicationAsMin)
		c.maxWritableVolumes = t.maxWritableVolumes
		return c
	}).(*Collection)
}
