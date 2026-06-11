package shell

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/stretchr/testify/assert"
)

func TestVolumeSelectionByDataCenter(t *testing.T) {
	topologyInfo := parseOutput(topoData)

	vids, err := collectVolumeIdsForTierChange(topologyInfo, 1000, types.ToDiskType(types.HddType), "dc2", "", 20.0, 0)
	assert.NoError(t, err)
	assert.Equal(t, 83, len(vids))

	vids, err = collectVolumeIdsForTierChange(topologyInfo, 1000, types.ToDiskType(types.HddType), "dc1", "", 20.0, 0)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(vids))
}

func TestFilterLocationsByDataCenter(t *testing.T) {
	_, allLocations := collectVolumeReplicaLocations(parseOutput(topoData))

	assert.Equal(t, 5, len(allLocations))
	assert.ElementsMatch(t, []string{"192.168.1.4:8080", "192.168.1.2:8080"}, locationNodeIds(filterLocationsByDataCenter(allLocations, "dc2")))
	assert.ElementsMatch(t, []string{"192.168.1.6:8080"}, locationNodeIds(filterLocationsByDataCenter(allLocations, "dc3")))
	assert.Empty(t, filterLocationsByDataCenter(allLocations, "dc9"))
}

func locationNodeIds(locations []location) (ids []string) {
	for _, loc := range locations {
		ids = append(ids, loc.dataNode.Id)
	}
	return
}
