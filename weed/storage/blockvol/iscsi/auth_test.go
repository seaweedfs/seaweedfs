package iscsi

import (
	"crypto/md5"
	"encoding/hex"
	"strconv"
	"strings"
	"testing"
)

// TestCHAP_LoginSuccess verifies that a correct CHAP username/password
// completes login through the full SecurityNeg -> LoginOp -> FFP flow.
func TestCHAP_LoginSuccess(t *testing.T) {
	config := DefaultTargetConfig()
	config.TargetName = "iqn.2024.com.seaweedfs:vol1"
	config.CHAPConfig = CHAPConfig{
		Enabled:  true,
		Username: "testuser",
		Secret:   "s3cret",
	}

	ln := NewLoginNegotiator(config)
	resolver := newResolver(config.TargetName)

	// PDU 1: Initiator sends SecurityNeg with AuthMethod=CHAP
	p1 := NewParams()
	p1.Set("InitiatorName", "iqn.2024.com.test:initiator1")
	p1.Set("TargetName", config.TargetName)
	p1.Set("AuthMethod", "CHAP")
	req1 := makeLoginReq(StageSecurityNeg, StageLoginOp, true, p1)

	resp1 := ln.HandleLoginPDU(req1, resolver)
	if resp1.LoginStatusClass() != LoginStatusSuccess {
		t.Fatalf("expected success, got class=%d detail=%d",
			resp1.LoginStatusClass(), resp1.LoginStatusDetail())
	}
	if resp1.LoginTransit() {
		t.Fatal("expected T=0 (no transit) in challenge response")
	}

	// Parse challenge params from response
	rp1, err := ParseParams(resp1.DataSegment)
	if err != nil {
		t.Fatalf("parse resp1 params: %v", err)
	}
	chapA, _ := rp1.Get("CHAP_A")
	if chapA != "5" {
		t.Fatalf("expected CHAP_A=5, got %q", chapA)
	}
	chapIStr, _ := rp1.Get("CHAP_I")
	chapCStr, _ := rp1.Get("CHAP_C")
	if chapIStr == "" || chapCStr == "" {
		t.Fatalf("missing CHAP_I or CHAP_C in response")
	}

	// Compute CHAP response
	chapID, _ := strconv.Atoi(chapIStr)
	challenge, _ := hex.DecodeString(strings.TrimPrefix(chapCStr, "0x"))
	h := md5.New()
	h.Write([]byte{byte(chapID)})
	h.Write([]byte("s3cret"))
	h.Write(challenge)
	chapR := "0x" + hex.EncodeToString(h.Sum(nil))

	// PDU 2: Initiator sends CHAP_N + CHAP_R
	p2 := NewParams()
	p2.Set("CHAP_N", "testuser")
	p2.Set("CHAP_R", chapR)
	req2 := makeLoginReq(StageSecurityNeg, StageLoginOp, true, p2)

	resp2 := ln.HandleLoginPDU(req2, resolver)
	if resp2.LoginStatusClass() != LoginStatusSuccess {
		t.Fatalf("expected success after CHAP, got class=%d detail=%d",
			resp2.LoginStatusClass(), resp2.LoginStatusDetail())
	}
	if !resp2.LoginTransit() {
		t.Fatal("expected T=1 (transit) after successful CHAP")
	}
	// Verify AuthMethod=CHAP echoed in the second response.
	rp2, err := ParseParams(resp2.DataSegment)
	if err != nil {
		t.Fatalf("parse resp2 params: %v", err)
	}
	if am, ok := rp2.Get("AuthMethod"); !ok || am != "CHAP" {
		t.Fatalf("expected AuthMethod=CHAP in second response, got %q (ok=%v)", am, ok)
	}

	// PDU 3: LoginOp -> FullFeature
	p3 := NewParams()
	p3.Set("MaxRecvDataSegmentLength", "65536")
	req3 := makeLoginReq(StageLoginOp, StageFullFeature, true, p3)

	resp3 := ln.HandleLoginPDU(req3, resolver)
	if resp3.LoginStatusClass() != LoginStatusSuccess {
		t.Fatalf("expected success at FFP, got class=%d detail=%d",
			resp3.LoginStatusClass(), resp3.LoginStatusDetail())
	}
	if !ln.Done() {
		t.Fatal("expected login Done after FFP transition")
	}
}

// TestCHAP_LoginWrongPassword verifies that an incorrect CHAP response
// is rejected with AuthFailure.
func TestCHAP_LoginWrongPassword(t *testing.T) {
	config := DefaultTargetConfig()
	config.TargetName = "iqn.2024.com.seaweedfs:vol1"
	config.CHAPConfig = CHAPConfig{
		Enabled:  true,
		Username: "testuser",
		Secret:   "s3cret",
	}

	ln := NewLoginNegotiator(config)
	resolver := newResolver(config.TargetName)

	// PDU 1: SecurityNeg with AuthMethod=CHAP
	p1 := NewParams()
	p1.Set("InitiatorName", "iqn.2024.com.test:initiator1")
	p1.Set("TargetName", config.TargetName)
	p1.Set("AuthMethod", "CHAP")
	req1 := makeLoginReq(StageSecurityNeg, StageLoginOp, true, p1)

	resp1 := ln.HandleLoginPDU(req1, resolver)
	if resp1.LoginStatusClass() != LoginStatusSuccess {
		t.Fatalf("expected success for challenge, got class=%d detail=%d",
			resp1.LoginStatusClass(), resp1.LoginStatusDetail())
	}

	// PDU 2: Wrong password
	p2 := NewParams()
	p2.Set("CHAP_N", "testuser")
	p2.Set("CHAP_R", "0xdeadbeefdeadbeefdeadbeefdeadbeef") // wrong
	req2 := makeLoginReq(StageSecurityNeg, StageLoginOp, true, p2)

	resp2 := ln.HandleLoginPDU(req2, resolver)
	if resp2.LoginStatusClass() != LoginStatusInitiatorErr ||
		resp2.LoginStatusDetail() != LoginDetailAuthFailure {
		t.Fatalf("expected AuthFailure, got class=%d detail=%d",
			resp2.LoginStatusClass(), resp2.LoginStatusDetail())
	}
}

// TestCHAP_DisabledAllowsLogin verifies that with CHAP disabled,
// AuthMethod=None works as before.
func TestCHAP_DisabledAllowsLogin(t *testing.T) {
	config := DefaultTargetConfig()
	config.TargetName = "iqn.2024.com.seaweedfs:vol1"
	// CHAPConfig.Enabled defaults to false

	ln := NewLoginNegotiator(config)
	resolver := newResolver(config.TargetName)

	// Single SecurityNeg PDU with transit to LoginOp
	p := NewParams()
	p.Set("InitiatorName", "iqn.2024.com.test:initiator1")
	p.Set("TargetName", config.TargetName)
	p.Set("AuthMethod", "None")
	req := makeLoginReq(StageSecurityNeg, StageLoginOp, true, p)

	resp := ln.HandleLoginPDU(req, resolver)
	if resp.LoginStatusClass() != LoginStatusSuccess {
		t.Fatalf("expected success, got class=%d detail=%d",
			resp.LoginStatusClass(), resp.LoginStatusDetail())
	}
	if !resp.LoginTransit() {
		t.Fatal("expected transit with CHAP disabled")
	}

	// LoginOp -> FFP
	p2 := NewParams()
	p2.Set("MaxRecvDataSegmentLength", "65536")
	req2 := makeLoginReq(StageLoginOp, StageFullFeature, true, p2)

	resp2 := ln.HandleLoginPDU(req2, resolver)
	if resp2.LoginStatusClass() != LoginStatusSuccess {
		t.Fatalf("expected success at FFP, got class=%d detail=%d",
			resp2.LoginStatusClass(), resp2.LoginStatusDetail())
	}
	if !ln.Done() {
		t.Fatal("expected login Done")
	}
}
