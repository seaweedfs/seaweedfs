package needle

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"testing"
)

func TestParseFileIdFromString(t *testing.T) {
	fidStr1 := "100,12345678"
	_, err := ParseFileIdFromString(fidStr1)
	if err == nil {
		t.Errorf("%s : KeyHash is too short", fidStr1)
	}

	fidStr1 = "100, 12345678"
	_, err = ParseFileIdFromString(fidStr1)
	if err == nil {
		t.Errorf("%s : needleId invalid syntax", fidStr1)
	}

	fidStr1 = "100,123456789"
	_, err = ParseFileIdFromString(fidStr1)
	if err != nil {
		t.Errorf("%s : should be OK", fidStr1)
	}

	var fileId *FileId
	fidStr1 = "100,123456789012345678901234"
	fileId, err = ParseFileIdFromString(fidStr1)
	if err != nil {
		t.Errorf("%s : should be OK", fidStr1)
	}
	if !(fileId.VolumeId == VolumeId(100) &&
		fileId.Key == types.NeedleId(0x1234567890123456) &&
		fileId.Cookie == types.Cookie(types.Uint32ToCookie(uint32(0x78901234)))) {
		t.Errorf("src : %s, dest : %v", fidStr1, fileId)
	}

	fidStr1 = "100,abcd0000abcd"
	fileId, err = ParseFileIdFromString(fidStr1)
	if err != nil {
		t.Errorf("%s : should be OK", fidStr1)
	}
	if !(fileId.VolumeId == VolumeId(100) &&
		fileId.Key == types.NeedleId(0xabcd) &&
		fileId.Cookie == types.Cookie(types.Uint32ToCookie(uint32(0xabcd)))) {
		t.Errorf("src : %s, dest : %v", fidStr1, fileId)
	}

	fidStr1 = "100,1234567890123456789012345"
	_, err = ParseFileIdFromString(fidStr1)
	if err == nil {
		t.Errorf("%s : needleId is too long", fidStr1)
	}
}
