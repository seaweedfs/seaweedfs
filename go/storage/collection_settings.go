package storage

type SettingKey int

const (
	keyReplicatePlacement SettingKey = iota
	keyGarbageThreshold
)

type CollectionSettings struct {
	settings map[string]map[SettingKey]interface{}
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

func (c *CollectionSettings) get(collection string, key SettingKey) interface{} {
	if m, ok := c.settings[collection]; ok {
		if v, ok := m[key]; ok {
			return v
		}
	}
	if m, ok := c.settings[""]; ok {
		return m[key]
	}
	return nil
}

func (c *CollectionSettings) set(collection string, key SettingKey, value interface{}) {
	m := c.settings[collection]
	if m == nil {
		m = make(map[SettingKey]interface{})
		c.settings[collection] = m
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

func (c *CollectionSettings) GetGarbageThreshold(collection string) string {
	return c.get(collection, keyGarbageThreshold).(string)
}

func (c *CollectionSettings) SetGarbageThreshold(collection string, gt string) {
	c.set(collection, keyGarbageThreshold, gt)
}

func (c *CollectionSettings) GetReplicaPlacement(collection string) *ReplicaPlacement {
	return c.get(collection, keyReplicatePlacement).(*ReplicaPlacement)
}

func (c *CollectionSettings) SetReplicaPlacement(collection, t string) error {
	rp, e := NewReplicaPlacementFromString(t)
	if e == nil {
		c.set(collection, keyReplicatePlacement, rp)
	}
	return e
}
