package storage

type SettingKey int

const (
	KeyReplicatePlacement SettingKey = iota
	KeyGarbageThreshold
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
	c.Set("", KeyReplicatePlacement, rp)
	c.Set("", KeyGarbageThreshold, defaultGarbageThreshold)
	return c
}

func (c *CollectionSettings) Get(collection string, key SettingKey) interface{} {
	if m, ok := c.settings[collection]; ok {
		if v, ok := m[key]; ok {
			return v
		}
	}
	if m, ok := c.settings[""]; ok {
		if v, ok := m[key]; ok {
			return v
		}
	}
	return nil
}

func (c *CollectionSettings) Set(collection string, key SettingKey, value interface{}) {
	if _, ok := c.settings[collection]; !ok {
		c.settings[collection] = make(map[SettingKey]interface{})
	}
	if value == nil {
		delete(c.settings[collection], key)
	}
}

func (c *CollectionSettings) GetGarbageThreshold(collection string) float32 {
	return c.Get(collection, KeyGarbageThreshold).(float32)
}

func (c *CollectionSettings) SetGarbageThreshold(collection string, gt float32) {
	c.Set(collection, KeyGarbageThreshold, gt)
}

func (c *CollectionSettings) GetReplicaPlacement(collection string) *ReplicaPlacement {
	return c.Get(collection, KeyReplicatePlacement).(*ReplicaPlacement)
}

func (c *CollectionSettings) SetReplicaPlacement(collection, t string) error {
	rp, e := NewReplicaPlacementFromString(t)
	if e == nil {
		c.Set(collection, KeyReplicatePlacement, rp)
	}
	return e
}
