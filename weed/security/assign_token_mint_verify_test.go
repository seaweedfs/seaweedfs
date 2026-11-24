package security

import (
	"testing"
	"time"
)

func TestAssignmentTokenBindsHolderAndUsesPreviousKey(t *testing.T) {
	current := SigningKey("assignment-current-key-0123456789")
	previous := SigningKey("assignment-previous-key-0123456789")
	fid := "3,0100000011"
	token := GenJwtForAssign(previous, time.Hour, fid, "session-1", 3, "")

	if !VerifyAssignJwt(current, previous, token, fid, "session-1", 0, "") {
		t.Fatal("assignment token signed by previous key was refused")
	}
	if !VerifyAssignJwt(nil, previous, token, fid, "session-1", 0, "") {
		t.Fatal("assignment token signed by previous key was refused when it was the only configured key")
	}
	if VerifyAssignJwt(current, previous, token, fid, "session-2", 0, "") {
		t.Fatal("assignment token was accepted for a different holder")
	}
	if VerifyAssignJwt(current, nil, token, fid, "session-1", 0, "") {
		t.Fatal("assignment token was accepted without its previous key")
	}
	if VerifyAssignJwt(nil, nil, token, fid, "session-1", 0, "") {
		t.Fatal("assignment token was accepted without any configured key")
	}
	forged := signAssignmentClaims(t, nil, SeaweedAssignClaims{
		Fid: fid, Count: 1,
		RegisteredClaims: jwtRegisteredClaims("session-1", time.Now().Add(time.Hour)),
	})
	if VerifyAssignJwt(nil, previous, forged, fid, "session-1", 0, "") {
		t.Fatal("empty-key signature was accepted during previous-only verification")
	}
}

func TestAssignmentTokenUsesDomainSeparatedSigningKey(t *testing.T) {
	key := SigningKey("assignment-domain-key-0123456789")
	fid := "3,0100000011"
	assignment := GenJwtForAssign(key, time.Hour, fid, "session-1", 1, "")

	if string(AssignSigningKey(key)) == string(key) {
		t.Fatal("assignment key is not domain separated")
	}
	if _, err := DecodeJwt(key, assignment, &SeaweedFilerClaims{}); err == nil {
		t.Fatal("assignment token was accepted as a filer token")
	}
	if _, err := DecodeJwt(key, assignment, &SeaweedFilerAdminClaims{}); err == nil {
		t.Fatal("assignment token was accepted as an admin token")
	}
	if _, err := DecodeJwt(key, assignment, &SeaweedFileIdClaims{}); err == nil {
		t.Fatal("assignment token was accepted as a volume token")
	}

	for name, token := range map[string]EncodedJwt{
		"filer":  GenJwtForFilerServer(key, 60),
		"scope":  GenJwtForFilerScope(key, time.Hour, "session-1", []string{"/scope"}, nil),
		"volume": GenJwtForVolumeServer(key, 60, fid),
	} {
		t.Run(name, func(t *testing.T) {
			if VerifyAssignJwt(key, nil, token, fid, "session-1", 0, "") {
				t.Fatal("non-assignment token was accepted as an assignment proof")
			}
		})
	}
}

func TestGenJwtForAssignRejectsInvalidInputs(t *testing.T) {
	key := SigningKey("assignment-input-key-0123456789")
	for name, input := range map[string]struct {
		key    SigningKey
		fid    string
		holder string
		count  int32
	}{
		"missing key":    {fid: "3,1", holder: "session-1", count: 1},
		"missing fid":    {key: key, holder: "session-1", count: 1},
		"missing holder": {key: key, fid: "3,1", count: 1},
		"zero count":     {key: key, fid: "3,1", holder: "session-1"},
		"negative count": {key: key, fid: "3,1", holder: "session-1", count: -1},
	} {
		t.Run(name, func(t *testing.T) {
			if token := GenJwtForAssign(input.key, time.Hour, input.fid, input.holder, input.count, ""); token != "" {
				t.Fatalf("GenJwtForAssign returned a token for invalid input")
			}
		})
	}
}
