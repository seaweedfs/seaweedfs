package storage

import "sync"

type SettingKey int

const (
	keyReplicatePlacement SettingKey = iota
	keyGarbageThreshold
)

type CollectionSettings struct {
	settings map[string]map[SettingKey]interface{}
	mutex    sync.RWMutex
}

func NewCollectionSettings(defaultReplicatePlacement, defaultGarbageThreshold string) *CollectionSettings {
	rp, e := NewReplicaPlacementFromString(defaultReplicatePlacement)
	if e != nil {
		rp, _ = NewReplicaPlacementFromString("000")
	}
	c := &CollectionSettings{
		settings: make(map[string]map[SettingKey]interface{}),
	}
	c.set("", keyReplicatePlacement, rp)
	c.set("", keyGarbageThreshold, defaultGarbageThreshold)
	return c
}

func (cs *CollectionSettings) get(collection string, key SettingKey) interface{} {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	if m, ok := cs.settings[collection]; ok {
		if v, ok := m[key]; ok {
			return v
		}
	}
	if m, ok := cs.settings[""]; ok {
		return m[key]
	}
	return nil
}

func (cs *CollectionSettings) set(collection string, key SettingKey, value interface{}) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	m := cs.settings[collection]
	if m == nil {
		m = make(map[SettingKey]interface{})
		cs.settings[collection] = m
	}
	if value == nil {
		//mustn't delete default setting
		if collection != "" {
			delete(m, key)
		}
	} else {
		m[key] = value
	}
}

func (cs *CollectionSettings) GetGarbageThreshold(collection string) string {
	return cs.get(collection, keyGarbageThreshold).(string)
}

func (cs *CollectionSettings) SetGarbageThreshold(collection string, gt string) {
	cs.set(collection, keyGarbageThreshold, gt)
}

func (cs *CollectionSettings) GetReplicaPlacement(collection string) *ReplicaPlacement {
	return cs.get(collection, keyReplicatePlacement).(*ReplicaPlacement)
}

func (cs *CollectionSettings) SetReplicaPlacement(collection, t string) error {
	rp, e := NewReplicaPlacementFromString(t)
	if e == nil {
		cs.set(collection, keyReplicatePlacement, rp)
	}
	return e
}
