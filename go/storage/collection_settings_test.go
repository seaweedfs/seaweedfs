package storage

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestCollectionSettings(t *testing.T) {
	cs1 := NewCollectionSettings("000", "0.3")
	cs1.SetReplicaPlacement("col1", "001")
	cs1.SetGarbageThreshold("col2", "0.5")

	if cs1.GetGarbageThreshold("col1") != "0.3" ||
		cs1.GetGarbageThreshold("col2") != "0.5" ||
		cs1.GetGarbageThreshold("") != "0.3" ||
		cs1.GetReplicaPlacement("").String() != "000" ||
		cs1.GetReplicaPlacement("col1").String() != "001" ||
		cs1.GetReplicaPlacement("col2").String() != "000" {
		t.Fatal("Value incorrect.")
	}
	pb := cs1.ToPbMessage()
	if buf, e := json.MarshalIndent(pb, "", "\t"); e == nil {
		t.Log(string(buf))
	} else {
		t.Fatal(e)
	}
	cs2 := NewCollectionSettingsFromPbMessage(pb)
	if !reflect.DeepEqual(cs1, cs2) {
		t.Fatal("PbMessage convert incorrect.")
	}
}
