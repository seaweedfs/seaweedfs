package needle

import "testing"

func TestNewVolumeId(t *testing.T) {
	if _, err := NewVolumeId("1"); err != nil {
		t.Error(err)
	}

	if _, err := NewVolumeId("a"); err != nil {
		t.Logf("a is not legal volume id, %v", err)
	}
}

func TestVolumeId_String(t *testing.T) {
	if str := VolumeId(10).String(); str != "10" {
		t.Errorf("to string failed")
	}

	vid := VolumeId(11)
	if str := vid.String(); str != "11" {
		t.Errorf("to string failed")
	}

	pvid := &vid
	if str := pvid.String(); str != "11" {
		t.Errorf("to string failed")
	}
}

func TestVolumeId_Next(t *testing.T) {
	if vid := VolumeId(10).Next(); vid != VolumeId(11) {
		t.Errorf("get next volume id failed")
	}

	vid := VolumeId(11)
	if new := vid.Next(); new != 12 {
		t.Errorf("get next volume id failed")
	}

	pvid := &vid
	if new := pvid.Next(); new != 12 {
		t.Errorf("get next volume id failed")
	}
}
