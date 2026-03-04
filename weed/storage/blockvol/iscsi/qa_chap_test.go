package iscsi

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
)

// TestQACHAP runs adversarial tests for CP5-3 CHAP authentication.
func TestQACHAP(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		// Config validation
		{name: "validate_empty_secret_rejected", run: testQACHAP_ValidateEmptySecret},
		{name: "validate_disabled_empty_secret_ok", run: testQACHAP_ValidateDisabledEmptyOK},

		// CHAP state machine
		{name: "verify_before_challenge_fails", run: testQACHAP_VerifyBeforeChallenge},
		{name: "double_verify_fails", run: testQACHAP_DoubleVerify},
		{name: "wrong_username_rejected", run: testQACHAP_WrongUsername},
		{name: "empty_username_config_accepts_any", run: testQACHAP_EmptyUsernameAcceptsAny},
		{name: "response_hex_case_insensitive", run: testQACHAP_HexCaseInsensitive},
		{name: "response_with_0x_prefix", run: testQACHAP_ResponseWith0xPrefix},
		{name: "response_with_0X_prefix", run: testQACHAP_ResponseWith0XPrefix},
		{name: "truncated_response_rejected", run: testQACHAP_TruncatedResponse},
		{name: "empty_response_rejected", run: testQACHAP_EmptyResponse},

		// Login flow integration
		{name: "login_chap_no_authmethod_offered", run: testQACHAP_LoginNoAuthMethod},
		{name: "login_chap_none_only_rejected", run: testQACHAP_LoginNoneOnlyRejected},
		{name: "login_chap_missing_chap_n", run: testQACHAP_LoginMissingChapN},
		{name: "login_chap_missing_chap_r", run: testQACHAP_LoginMissingChapR},
		{name: "login_chap_replayed_challenge", run: testQACHAP_LoginReplayedChallenge},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

// --- Config validation ---

func testQACHAP_ValidateEmptySecret(t *testing.T) {
	err := ValidateCHAPConfig(CHAPConfig{Enabled: true, Secret: ""})
	if err != ErrCHAPSecretEmpty {
		t.Fatalf("expected ErrCHAPSecretEmpty, got %v", err)
	}
}

func testQACHAP_ValidateDisabledEmptyOK(t *testing.T) {
	err := ValidateCHAPConfig(CHAPConfig{Enabled: false, Secret: ""})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

// --- CHAP state machine ---

func testQACHAP_VerifyBeforeChallenge(t *testing.T) {
	auth := NewCHAPAuthenticator(CHAPConfig{Enabled: true, Secret: "secret"})
	// Verify without generating challenge should fail.
	if auth.Verify("user", "deadbeef") {
		t.Fatal("Verify before GenerateChallenge should return false")
	}
}

func testQACHAP_DoubleVerify(t *testing.T) {
	auth := NewCHAPAuthenticator(CHAPConfig{Enabled: true, Secret: "secret"})
	params, err := auth.GenerateChallenge()
	if err != nil {
		t.Fatal(err)
	}

	// Compute correct response.
	resp := computeCHAPResponse(auth.id, "secret", auth.challenge)

	// First verify should succeed.
	if !auth.Verify("", resp) {
		t.Fatal("first Verify should succeed")
	}

	// Second verify should fail (state is chapDone, not chapChallengeSent).
	if auth.Verify("", resp) {
		t.Fatal("second Verify should fail (state already chapDone)")
	}
	_ = params
}

func testQACHAP_WrongUsername(t *testing.T) {
	auth := NewCHAPAuthenticator(CHAPConfig{
		Enabled: true, Username: "correctuser", Secret: "secret",
	})
	auth.GenerateChallenge()
	resp := computeCHAPResponse(auth.id, "secret", auth.challenge)

	if auth.Verify("wronguser", resp) {
		t.Fatal("wrong username should be rejected")
	}
}

func testQACHAP_EmptyUsernameAcceptsAny(t *testing.T) {
	auth := NewCHAPAuthenticator(CHAPConfig{
		Enabled: true, Username: "", Secret: "secret",
	})
	auth.GenerateChallenge()
	resp := computeCHAPResponse(auth.id, "secret", auth.challenge)

	if !auth.Verify("anyuser", resp) {
		t.Fatal("empty username config should accept any username")
	}
}

func testQACHAP_HexCaseInsensitive(t *testing.T) {
	auth := NewCHAPAuthenticator(CHAPConfig{Enabled: true, Secret: "secret"})
	auth.GenerateChallenge()
	resp := computeCHAPResponse(auth.id, "secret", auth.challenge)

	// Upper-case the response.
	upper := strings.ToUpper(resp)
	if !auth.Verify("", upper) {
		t.Fatal("hex comparison should be case-insensitive")
	}
}

func testQACHAP_ResponseWith0xPrefix(t *testing.T) {
	auth := NewCHAPAuthenticator(CHAPConfig{Enabled: true, Secret: "secret"})
	auth.GenerateChallenge()
	resp := computeCHAPResponse(auth.id, "secret", auth.challenge)

	if !auth.Verify("", "0x"+resp) {
		t.Fatal("response with 0x prefix should be accepted")
	}
}

func testQACHAP_ResponseWith0XPrefix(t *testing.T) {
	auth := NewCHAPAuthenticator(CHAPConfig{Enabled: true, Secret: "secret"})
	auth.GenerateChallenge()
	resp := computeCHAPResponse(auth.id, "secret", auth.challenge)

	if !auth.Verify("", "0X"+resp) {
		t.Fatal("response with 0X prefix should be accepted")
	}
}

func testQACHAP_TruncatedResponse(t *testing.T) {
	auth := NewCHAPAuthenticator(CHAPConfig{Enabled: true, Secret: "secret"})
	auth.GenerateChallenge()
	resp := computeCHAPResponse(auth.id, "secret", auth.challenge)

	// Truncate to half.
	if auth.Verify("", resp[:len(resp)/2]) {
		t.Fatal("truncated response should be rejected")
	}
}

func testQACHAP_EmptyResponse(t *testing.T) {
	auth := NewCHAPAuthenticator(CHAPConfig{Enabled: true, Secret: "secret"})
	auth.GenerateChallenge()

	if auth.Verify("", "") {
		t.Fatal("empty response should be rejected")
	}
}

// --- Login flow integration ---

// mkResolver returns a TargetResolver that accepts the given target name.
type simpleResolver struct{ name string }

func (r *simpleResolver) HasTarget(n string) bool { return n == r.name }

func testQACHAP_LoginNoAuthMethod(t *testing.T) {
	config := DefaultTargetConfig()
	config.TargetName = "iqn.test:target"
	config.CHAPConfig = CHAPConfig{Enabled: true, Secret: "secret"}
	ln := NewLoginNegotiator(config)

	// First PDU: SecurityNeg with InitiatorName but no AuthMethod.
	req := buildLoginPDU(StageSecurityNeg, StageLoginOp, false, map[string]string{
		"InitiatorName": "iqn.test:init",
		"TargetName":    "iqn.test:target",
	})
	resp := ln.HandleLoginPDU(req, &simpleResolver{"iqn.test:target"})
	class, detail := resp.LoginStatusClass(), resp.LoginStatusDetail()
	if class != LoginStatusInitiatorErr || detail != LoginDetailAuthFailure {
		t.Fatalf("expected auth failure, got class=%d detail=%d", class, detail)
	}
}

func testQACHAP_LoginNoneOnlyRejected(t *testing.T) {
	config := DefaultTargetConfig()
	config.TargetName = "iqn.test:target"
	config.CHAPConfig = CHAPConfig{Enabled: true, Secret: "secret"}
	ln := NewLoginNegotiator(config)

	// Initiator only offers "None".
	req := buildLoginPDU(StageSecurityNeg, StageLoginOp, false, map[string]string{
		"InitiatorName": "iqn.test:init",
		"TargetName":    "iqn.test:target",
		"AuthMethod":    "None",
	})
	resp := ln.HandleLoginPDU(req, &simpleResolver{"iqn.test:target"})
	class, detail := resp.LoginStatusClass(), resp.LoginStatusDetail()
	if class != LoginStatusInitiatorErr || detail != LoginDetailAuthFailure {
		t.Fatalf("expected auth failure for None-only, got class=%d detail=%d", class, detail)
	}
}

func testQACHAP_LoginMissingChapN(t *testing.T) {
	config := DefaultTargetConfig()
	config.TargetName = "iqn.test:target"
	config.CHAPConfig = CHAPConfig{Enabled: true, Secret: "secret"}
	ln := NewLoginNegotiator(config)
	resolver := &simpleResolver{"iqn.test:target"}

	// Step 1: send AuthMethod=CHAP, get challenge back.
	req1 := buildLoginPDU(StageSecurityNeg, StageLoginOp, false, map[string]string{
		"InitiatorName": "iqn.test:init",
		"TargetName":    "iqn.test:target",
		"AuthMethod":    "CHAP",
	})
	resp1 := ln.HandleLoginPDU(req1, resolver)
	class1, _ := resp1.LoginStatusClass(), resp1.LoginStatusDetail()
	if class1 != LoginStatusSuccess {
		t.Fatalf("step 1: expected success, got class=%d", class1)
	}

	// Step 2: send CHAP_R but no CHAP_N.
	req2 := buildLoginPDU(StageSecurityNeg, StageLoginOp, true, map[string]string{
		"CHAP_R": "deadbeef",
	})
	resp2 := ln.HandleLoginPDU(req2, resolver)
	class2, detail2 := resp2.LoginStatusClass(), resp2.LoginStatusDetail()
	if class2 != LoginStatusInitiatorErr || detail2 != LoginDetailAuthFailure {
		t.Fatalf("expected auth failure for missing CHAP_N, got class=%d detail=%d", class2, detail2)
	}
}

func testQACHAP_LoginMissingChapR(t *testing.T) {
	config := DefaultTargetConfig()
	config.TargetName = "iqn.test:target"
	config.CHAPConfig = CHAPConfig{Enabled: true, Secret: "secret"}
	ln := NewLoginNegotiator(config)
	resolver := &simpleResolver{"iqn.test:target"}

	req1 := buildLoginPDU(StageSecurityNeg, StageLoginOp, false, map[string]string{
		"InitiatorName": "iqn.test:init",
		"TargetName":    "iqn.test:target",
		"AuthMethod":    "CHAP",
	})
	ln.HandleLoginPDU(req1, resolver)

	// Step 2: send CHAP_N but no CHAP_R.
	req2 := buildLoginPDU(StageSecurityNeg, StageLoginOp, true, map[string]string{
		"CHAP_N": "user",
	})
	resp2 := ln.HandleLoginPDU(req2, resolver)
	class2, detail2 := resp2.LoginStatusClass(), resp2.LoginStatusDetail()
	if class2 != LoginStatusInitiatorErr || detail2 != LoginDetailAuthFailure {
		t.Fatalf("expected auth failure for missing CHAP_R, got class=%d detail=%d", class2, detail2)
	}
}

func testQACHAP_LoginReplayedChallenge(t *testing.T) {
	config := DefaultTargetConfig()
	config.TargetName = "iqn.test:target"
	config.CHAPConfig = CHAPConfig{Enabled: true, Secret: "secret"}
	resolver := &simpleResolver{"iqn.test:target"}

	// Session 1: complete login.
	ln1 := NewLoginNegotiator(config)
	req1a := buildLoginPDU(StageSecurityNeg, StageLoginOp, false, map[string]string{
		"InitiatorName": "iqn.test:init",
		"TargetName":    "iqn.test:target",
		"AuthMethod":    "CHAP",
	})
	resp1a := ln1.HandleLoginPDU(req1a, resolver)
	params1a, _ := ParseParams(resp1a.DataSegment)
	chapC1, _ := params1a.Get("CHAP_C")
	chapI1, _ := params1a.Get("CHAP_I")

	// Compute valid response for session 1.
	id1 := parseChapID(chapI1)
	challenge1 := parseChapChallenge(chapC1)
	validResp := computeCHAPResponse(id1, "secret", challenge1)

	// Complete session 1.
	req1b := buildLoginPDU(StageSecurityNeg, StageFullFeature, true, map[string]string{
		"CHAP_N": "user",
		"CHAP_R": validResp,
	})
	resp1b := ln1.HandleLoginPDU(req1b, resolver)
	class1b, _ := resp1b.LoginStatusClass(), resp1b.LoginStatusDetail()
	if class1b != LoginStatusSuccess {
		t.Fatalf("session 1 should succeed, got class=%d", class1b)
	}

	// Session 2: try to replay session 1's response.
	ln2 := NewLoginNegotiator(config)
	req2a := buildLoginPDU(StageSecurityNeg, StageLoginOp, false, map[string]string{
		"InitiatorName": "iqn.test:init",
		"TargetName":    "iqn.test:target",
		"AuthMethod":    "CHAP",
	})
	ln2.HandleLoginPDU(req2a, resolver)

	// Replay session 1's response on session 2 (different challenge).
	req2b := buildLoginPDU(StageSecurityNeg, StageFullFeature, true, map[string]string{
		"CHAP_N": "user",
		"CHAP_R": validResp,
	})
	resp2b := ln2.HandleLoginPDU(req2b, resolver)
	class2b, detail2b := resp2b.LoginStatusClass(), resp2b.LoginStatusDetail()
	if class2b != LoginStatusInitiatorErr || detail2b != LoginDetailAuthFailure {
		t.Fatalf("replayed response should be rejected, got class=%d detail=%d", class2b, detail2b)
	}
}

// --- Helpers ---

func computeCHAPResponse(id uint8, secret string, challenge []byte) string {
	h := md5.New()
	h.Write([]byte{id})
	h.Write([]byte(secret))
	h.Write(challenge)
	return hex.EncodeToString(h.Sum(nil))
}

func parseChapID(s string) uint8 {
	var id int
	fmt.Sscanf(s, "%d", &id)
	return uint8(id)
}

func parseChapChallenge(s string) []byte {
	s = strings.TrimPrefix(s, "0x")
	s = strings.TrimPrefix(s, "0X")
	b, _ := hex.DecodeString(s)
	return b
}

func buildLoginPDU(csg, nsg uint8, transit bool, kvs map[string]string) *PDU {
	pdu := &PDU{}
	pdu.SetOpcode(OpLoginReq)
	pdu.SetLoginStages(csg, nsg)
	pdu.SetLoginTransit(transit)
	pdu.SetInitiatorTaskTag(1)

	params := NewParams()
	for k, v := range kvs {
		params.Set(k, v)
	}
	pdu.DataSegment = params.Encode()
	return pdu
}
