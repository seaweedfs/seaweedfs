package storage

import "github.com/chrislusf/seaweedfs/weed/weedpb"

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

func NewCollectionSettingsFromPbMessage(msg []*weedpb.CollectionSetting) *CollectionSettings {
	c := &CollectionSettings{
		settings: make(map[string]map[SettingKey]interface{}),
	}
	for _, m := range msg {
		c.SetGarbageThreshold(m.Collection, m.VacuumGarbageThreshold)
		c.SetReplicaPlacement(m.Collection, m.ReplicaPlacement)
	}
	return c
}

func (cs *CollectionSettings) ToPbMessage() []*weedpb.CollectionSetting {
	msg := make([]*weedpb.CollectionSetting, 0, len(cs.settings))
	for collection, m := range cs.settings {
		setting := &weedpb.CollectionSetting{
			Collection: collection,
		}
		if v, ok := m[keyReplicatePlacement]; ok && v != nil {
			setting.ReplicaPlacement = v.(*ReplicaPlacement).String()
		}
		if v, ok := m[keyGarbageThreshold]; ok && v != nil {
			setting.VacuumGarbageThreshold = v.(string)
		}
		msg = append(msg, setting)
	}
	return msg
}

func (cs *CollectionSettings) get(collection string, key SettingKey) interface{} {
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
	if gt == "" {
		cs.set(collection, keyGarbageThreshold, nil)
	} else {
		cs.set(collection, keyGarbageThreshold, gt)

	}
}

func (cs *CollectionSettings) GetReplicaPlacement(collection string) *ReplicaPlacement {
	v := cs.get(collection, keyReplicatePlacement)
	if v == nil {
		return nil
	} else {
		return v.(*ReplicaPlacement)
	}
}

func (cs *CollectionSettings) SetReplicaPlacement(collection, t string) error {
	if t == "" {
		cs.set(collection, keyReplicatePlacement, nil)
		return nil
	}
	rp, e := NewReplicaPlacementFromString(t)
	if e == nil {
		cs.set(collection, keyReplicatePlacement, rp)
	}
	return e
}
