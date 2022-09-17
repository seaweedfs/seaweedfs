package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

func TestVolumesBinaryState(t *testing.T) {
	vids := []needle.VolumeId{
		needle.VolumeId(1),
		needle.VolumeId(2),
		needle.VolumeId(3),
		needle.VolumeId(4),
		needle.VolumeId(5),
	}

	dns := []*DataNode{
		&DataNode{
			Ip:   "127.0.0.1",
			Port: 8081,
		},
		&DataNode{
			Ip:   "127.0.0.1",
			Port: 8082,
		},
		&DataNode{
			Ip:   "127.0.0.1",
			Port: 8083,
		},
	}

	rp, _ := super_block.NewReplicaPlacementFromString("002")

	state_exist := NewVolumesBinaryState(readOnlyState, rp, ExistCopies())
	state_exist.Add(vids[0], dns[0])
	state_exist.Add(vids[0], dns[1])
	state_exist.Add(vids[1], dns[2])
	state_exist.Add(vids[2], dns[1])
	state_exist.Add(vids[4], dns[1])
	state_exist.Add(vids[4], dns[2])

	state_no := NewVolumesBinaryState(readOnlyState, rp, NoCopies())
	state_no.Add(vids[0], dns[0])
	state_no.Add(vids[0], dns[1])
	state_no.Add(vids[3], dns[1])

	tests := []struct {
		name                    string
		state                   *volumesBinaryState
		expectResult            []bool
		update                  func()
		expectResultAfterUpdate []bool
	}{
		{
			name:         "mark true when copies exist",
			state:        state_exist,
			expectResult: []bool{true, true, true, false, true},
			update: func() {
				state_exist.Remove(vids[0], dns[2])
				state_exist.Remove(vids[1], dns[2])
				state_exist.Remove(vids[3], dns[2])
				state_exist.Remove(vids[4], dns[1])
				state_exist.Remove(vids[4], dns[2])
			},
			expectResultAfterUpdate: []bool{true, false, true, false, false},
		},
		{
			name:         "mark true when no copies exist",
			state:        state_no,
			expectResult: []bool{false, true, true, false, true},
			update: func() {
				state_no.Remove(vids[0], dns[2])
				state_no.Remove(vids[1], dns[2])
				state_no.Add(vids[2], dns[1])
				state_no.Remove(vids[3], dns[1])
				state_no.Remove(vids[4], dns[2])
			},
			expectResultAfterUpdate: []bool{false, true, false, true, true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var result []bool
			for index, _ := range vids {
				result = append(result, test.state.IsTrue(vids[index]))
			}
			if len(result) != len(test.expectResult) {
				t.Fatalf("len(result) != len(expectResult), got %d, expected %d\n",
					len(result), len(test.expectResult))
			}
			for index, val := range result {
				if val != test.expectResult[index] {
					t.Fatalf("result not matched, index %d, got %v, expected %v\n",
						index, val, test.expectResult[index])
				}
			}
			test.update()
			var updateResult []bool
			for index, _ := range vids {
				updateResult = append(updateResult, test.state.IsTrue(vids[index]))
			}
			if len(updateResult) != len(test.expectResultAfterUpdate) {
				t.Fatalf("len(updateResult) != len(expectResultAfterUpdate), got %d, expected %d\n",
					len(updateResult), len(test.expectResultAfterUpdate))
			}
			for index, val := range updateResult {
				if val != test.expectResultAfterUpdate[index] {
					t.Fatalf("update result not matched, index %d, got %v, expected %v\n",
						index, val, test.expectResultAfterUpdate[index])
				}
			}
		})
	}
}
