package iscsi

import (
	"testing"
)

// mockResolver implements TargetResolver for tests.
type mockResolver struct {
	targets map[string]bool
}

func (m *mockResolver) HasTarget(name string) bool {
	return m.targets[name]
}

func newResolver(names ...string) *mockResolver {
	r := &mockResolver{targets: make(map[string]bool)}
	for _, n := range names {
		r.targets[n] = true
	}
	return r
}

const testTargetName = "iqn.2024.com.seaweedfs:vol1"
const testInitiatorName = "iqn.2024.com.test:client1"

func TestLogin(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{"single_pdu_security_to_ffp", testSinglePDUSecurityToFFP},
		{"two_phase_login", testTwoPhaseLogin},
		{"security_to_loginop_to_ffp", testSecurityToLoginOpToFFP},
		{"missing_initiator_name", testMissingInitiatorName},
		{"target_not_found", testTargetNotFound},
		{"discovery_session_no_target", testDiscoverySessionNoTarget},
		{"wrong_opcode", testWrongOpcode},
		{"operational_negotiation", testOperationalNegotiation},
		{"redirect_permanent", testRedirectPermanent},
		{"redirect_temporary", testRedirectTemporary},
		{"login_result", testLoginResult},
		{"unknown_key_not_understood", testUnknownKeyNotUnderstood},
		{"header_data_digest_none", testHeaderDataDigestNone},
		{"duplicate_login_pdu", testDuplicateLoginPDU},
		{"invalid_csg", testInvalidCSG},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

func makeLoginReq(csg, nsg uint8, transit bool, params *Params) *PDU {
	p := &PDU{}
	p.SetOpcode(OpLoginReq)
	p.SetLoginStages(csg, nsg)
	if transit {
		p.SetLoginTransit(true)
	}
	isid := [6]byte{0x00, 0x02, 0x3D, 0x00, 0x00, 0x01}
	p.SetISID(isid)
	if params != nil {
		p.DataSegment = params.Encode()
	}
	return p
}

func testSinglePDUSecurityToFFP(t *testing.T) {
	ln := NewLoginNegotiator(DefaultTargetConfig())
	resolver := newResolver(testTargetName)

	params := NewParams()
	params.Set("InitiatorName", testInitiatorName)
	params.Set("TargetName", testTargetName)
	params.Set("SessionType", "Normal")

	req := makeLoginReq(StageSecurityNeg, StageFullFeature, true, params)
	resp := ln.HandleLoginPDU(req, resolver)

	if resp.LoginStatusClass() != LoginStatusSuccess {
		t.Fatalf("status class: %d", resp.LoginStatusClass())
	}
	if !resp.LoginTransit() {
		t.Fatal("transit should be set")
	}
	if !ln.Done() {
		t.Fatal("should be done")
	}
	if resp.TSIH() == 0 {
		t.Fatal("TSIH should be assigned")
	}

	// Verify AuthMethod=None in response
	rp, err := ParseParams(resp.DataSegment)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := rp.Get("AuthMethod"); !ok || v != "None" {
		t.Fatalf("AuthMethod: %q, %v", v, ok)
	}
}

func testTwoPhaseLogin(t *testing.T) {
	ln := NewLoginNegotiator(DefaultTargetConfig())
	resolver := newResolver(testTargetName)

	// Phase 1: Security -> LoginOp
	params := NewParams()
	params.Set("InitiatorName", testInitiatorName)
	params.Set("TargetName", testTargetName)
	params.Set("SessionType", "Normal")

	req := makeLoginReq(StageSecurityNeg, StageLoginOp, true, params)
	resp := ln.HandleLoginPDU(req, resolver)

	if resp.LoginStatusClass() != LoginStatusSuccess {
		t.Fatalf("phase 1 status: %d", resp.LoginStatusClass())
	}
	if ln.Done() {
		t.Fatal("should not be done yet")
	}

	// Phase 2: LoginOp -> FFP
	params2 := NewParams()
	params2.Set("MaxRecvDataSegmentLength", "65536")

	req2 := makeLoginReq(StageLoginOp, StageFullFeature, true, params2)
	resp2 := ln.HandleLoginPDU(req2, resolver)

	if resp2.LoginStatusClass() != LoginStatusSuccess {
		t.Fatalf("phase 2 status: %d", resp2.LoginStatusClass())
	}
	if !ln.Done() {
		t.Fatal("should be done")
	}
}

func testSecurityToLoginOpToFFP(t *testing.T) {
	ln := NewLoginNegotiator(DefaultTargetConfig())
	resolver := newResolver(testTargetName)

	// Security (no transit)
	params := NewParams()
	params.Set("InitiatorName", testInitiatorName)
	params.Set("TargetName", testTargetName)

	req := makeLoginReq(StageSecurityNeg, StageSecurityNeg, false, params)
	resp := ln.HandleLoginPDU(req, resolver)
	if resp.LoginStatusClass() != LoginStatusSuccess {
		t.Fatalf("status: %d", resp.LoginStatusClass())
	}

	// Security -> LoginOp (transit)
	req2 := makeLoginReq(StageSecurityNeg, StageLoginOp, true, NewParams())
	resp2 := ln.HandleLoginPDU(req2, resolver)
	if resp2.LoginStatusClass() != LoginStatusSuccess {
		t.Fatalf("status: %d", resp2.LoginStatusClass())
	}

	// LoginOp -> FFP (transit)
	req3 := makeLoginReq(StageLoginOp, StageFullFeature, true, NewParams())
	resp3 := ln.HandleLoginPDU(req3, resolver)
	if resp3.LoginStatusClass() != LoginStatusSuccess {
		t.Fatalf("status: %d", resp3.LoginStatusClass())
	}
	if !ln.Done() {
		t.Fatal("should be done")
	}
}

func testMissingInitiatorName(t *testing.T) {
	ln := NewLoginNegotiator(DefaultTargetConfig())
	resolver := newResolver(testTargetName)

	// No InitiatorName
	params := NewParams()
	params.Set("TargetName", testTargetName)

	req := makeLoginReq(StageSecurityNeg, StageFullFeature, true, params)
	resp := ln.HandleLoginPDU(req, resolver)

	if resp.LoginStatusClass() != LoginStatusInitiatorErr {
		t.Fatalf("expected initiator error, got %d", resp.LoginStatusClass())
	}
	if resp.LoginStatusDetail() != LoginDetailMissingParam {
		t.Fatalf("expected missing param, got %d", resp.LoginStatusDetail())
	}
}

func testTargetNotFound(t *testing.T) {
	ln := NewLoginNegotiator(DefaultTargetConfig())
	resolver := newResolver() // empty

	params := NewParams()
	params.Set("InitiatorName", testInitiatorName)
	params.Set("TargetName", "iqn.2024.com.seaweedfs:nonexistent")

	req := makeLoginReq(StageSecurityNeg, StageFullFeature, true, params)
	resp := ln.HandleLoginPDU(req, resolver)

	if resp.LoginStatusClass() != LoginStatusInitiatorErr {
		t.Fatalf("expected initiator error, got %d", resp.LoginStatusClass())
	}
	if resp.LoginStatusDetail() != LoginDetailNotFound {
		t.Fatalf("expected not found, got %d", resp.LoginStatusDetail())
	}
}

func testDiscoverySessionNoTarget(t *testing.T) {
	ln := NewLoginNegotiator(DefaultTargetConfig())

	params := NewParams()
	params.Set("InitiatorName", testInitiatorName)
	params.Set("SessionType", "Discovery")

	req := makeLoginReq(StageSecurityNeg, StageFullFeature, true, params)
	resp := ln.HandleLoginPDU(req, nil) // no resolver needed

	if resp.LoginStatusClass() != LoginStatusSuccess {
		t.Fatalf("status: %d/%d", resp.LoginStatusClass(), resp.LoginStatusDetail())
	}
	if !ln.Done() {
		t.Fatal("should be done")
	}
	if ln.SessionType != "Discovery" {
		t.Fatalf("session type: %q", ln.SessionType)
	}
}

func testWrongOpcode(t *testing.T) {
	ln := NewLoginNegotiator(DefaultTargetConfig())

	req := &PDU{}
	req.SetOpcode(OpSCSICmd) // wrong
	resp := ln.HandleLoginPDU(req, nil)

	if resp.LoginStatusClass() != LoginStatusInitiatorErr {
		t.Fatalf("expected error, got %d", resp.LoginStatusClass())
	}
}

func testOperationalNegotiation(t *testing.T) {
	config := DefaultTargetConfig()
	config.TargetAlias = "test-target"
	ln := NewLoginNegotiator(config)
	resolver := newResolver(testTargetName)

	// First: security phase
	params := NewParams()
	params.Set("InitiatorName", testInitiatorName)
	params.Set("TargetName", testTargetName)
	req := makeLoginReq(StageSecurityNeg, StageLoginOp, true, params)
	ln.HandleLoginPDU(req, resolver)

	// Second: operational negotiation
	params2 := NewParams()
	params2.Set("MaxRecvDataSegmentLength", "65536")
	params2.Set("MaxBurstLength", "131072")
	params2.Set("FirstBurstLength", "32768")
	params2.Set("InitialR2T", "Yes")
	params2.Set("ImmediateData", "No")
	params2.Set("HeaderDigest", "CRC32C,None")
	params2.Set("DataDigest", "CRC32C,None")

	req2 := makeLoginReq(StageLoginOp, StageFullFeature, true, params2)
	resp := ln.HandleLoginPDU(req2, resolver)

	if resp.LoginStatusClass() != LoginStatusSuccess {
		t.Fatalf("status: %d", resp.LoginStatusClass())
	}

	rp, err := ParseParams(resp.DataSegment)
	if err != nil {
		t.Fatal(err)
	}

	// Verify negotiated values
	if v, ok := rp.Get("HeaderDigest"); !ok || v != "None" {
		t.Fatalf("HeaderDigest: %q", v)
	}
	if v, ok := rp.Get("DataDigest"); !ok || v != "None" {
		t.Fatalf("DataDigest: %q", v)
	}
	if v, ok := rp.Get("InitialR2T"); !ok || v != "Yes" {
		t.Fatalf("InitialR2T: %q", v)
	}
	if v, ok := rp.Get("ImmediateData"); !ok || v != "No" {
		t.Fatalf("ImmediateData: %q", v)
	}
	if v, ok := rp.Get("TargetAlias"); !ok || v != "test-target" {
		t.Fatalf("TargetAlias: %q", v)
	}

	// Check negotiated state
	if ln.NegInitialR2T != true {
		t.Fatal("NegInitialR2T should be true")
	}
	if ln.NegImmediateData != false {
		t.Fatal("NegImmediateData should be false")
	}
}

func testRedirectPermanent(t *testing.T) {
	req := makeLoginReq(StageSecurityNeg, StageFullFeature, true, NewParams())
	resp := BuildRedirectResponse(req, "10.0.0.1:3260,1", true)

	if resp.LoginStatusClass() != LoginStatusRedirect {
		t.Fatalf("class: %d", resp.LoginStatusClass())
	}
	if resp.LoginStatusDetail() != LoginDetailTargetMoved {
		t.Fatalf("detail: %d", resp.LoginStatusDetail())
	}

	rp, err := ParseParams(resp.DataSegment)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := rp.Get("TargetAddress"); v != "10.0.0.1:3260,1" {
		t.Fatalf("TargetAddress: %q", v)
	}
}

func testRedirectTemporary(t *testing.T) {
	req := makeLoginReq(StageSecurityNeg, StageFullFeature, true, NewParams())
	resp := BuildRedirectResponse(req, "192.168.1.100:3260,2", false)

	if resp.LoginStatusDetail() != LoginDetailTargetMovedTemp {
		t.Fatalf("detail: %d", resp.LoginStatusDetail())
	}
}

func testLoginResult(t *testing.T) {
	ln := NewLoginNegotiator(DefaultTargetConfig())
	resolver := newResolver(testTargetName)

	params := NewParams()
	params.Set("InitiatorName", testInitiatorName)
	params.Set("TargetName", testTargetName)
	req := makeLoginReq(StageSecurityNeg, StageFullFeature, true, params)
	ln.HandleLoginPDU(req, resolver)

	result := ln.Result()
	if result.InitiatorName != testInitiatorName {
		t.Fatalf("initiator: %q", result.InitiatorName)
	}
	if result.SessionType != "Normal" {
		t.Fatalf("session type: %q", result.SessionType)
	}
	if result.TSIH == 0 {
		t.Fatal("TSIH should be assigned")
	}
}

func testUnknownKeyNotUnderstood(t *testing.T) {
	ln := NewLoginNegotiator(DefaultTargetConfig())
	resolver := newResolver(testTargetName)

	// Security phase first
	params := NewParams()
	params.Set("InitiatorName", testInitiatorName)
	params.Set("TargetName", testTargetName)
	req := makeLoginReq(StageSecurityNeg, StageLoginOp, true, params)
	ln.HandleLoginPDU(req, resolver)

	// Op phase with unknown key
	params2 := NewParams()
	params2.Set("X-CustomKey", "whatever")
	req2 := makeLoginReq(StageLoginOp, StageFullFeature, true, params2)
	resp := ln.HandleLoginPDU(req2, resolver)

	rp, err := ParseParams(resp.DataSegment)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := rp.Get("X-CustomKey"); !ok || v != "NotUnderstood" {
		t.Fatalf("expected NotUnderstood, got %q, %v", v, ok)
	}
}

func testHeaderDataDigestNone(t *testing.T) {
	ln := NewLoginNegotiator(DefaultTargetConfig())
	resolver := newResolver(testTargetName)

	params := NewParams()
	params.Set("InitiatorName", testInitiatorName)
	params.Set("TargetName", testTargetName)
	req := makeLoginReq(StageSecurityNeg, StageLoginOp, true, params)
	ln.HandleLoginPDU(req, resolver)

	params2 := NewParams()
	params2.Set("HeaderDigest", "CRC32C")
	params2.Set("DataDigest", "CRC32C")
	req2 := makeLoginReq(StageLoginOp, StageFullFeature, true, params2)
	resp := ln.HandleLoginPDU(req2, resolver)

	rp, _ := ParseParams(resp.DataSegment)
	if v, _ := rp.Get("HeaderDigest"); v != "None" {
		t.Fatalf("HeaderDigest: %q (we always negotiate None)", v)
	}
	if v, _ := rp.Get("DataDigest"); v != "None" {
		t.Fatalf("DataDigest: %q", v)
	}
}

func testDuplicateLoginPDU(t *testing.T) {
	ln := NewLoginNegotiator(DefaultTargetConfig())
	resolver := newResolver(testTargetName)

	params := NewParams()
	params.Set("InitiatorName", testInitiatorName)
	params.Set("TargetName", testTargetName)

	// Send same security PDU twice (no transit)
	req := makeLoginReq(StageSecurityNeg, StageSecurityNeg, false, params)
	resp1 := ln.HandleLoginPDU(req, resolver)
	if resp1.LoginStatusClass() != LoginStatusSuccess {
		t.Fatalf("first: %d", resp1.LoginStatusClass())
	}

	// Same stage again should still work
	resp2 := ln.HandleLoginPDU(req, resolver)
	if resp2.LoginStatusClass() != LoginStatusSuccess {
		t.Fatalf("second: %d", resp2.LoginStatusClass())
	}
}

func testInvalidCSG(t *testing.T) {
	ln := NewLoginNegotiator(DefaultTargetConfig())

	// CSG=FullFeature (3) is invalid as a current stage
	req := &PDU{}
	req.SetOpcode(OpLoginReq)
	req.SetLoginStages(StageFullFeature, StageFullFeature)
	req.SetLoginTransit(true)
	isid := [6]byte{0x00, 0x02, 0x3D, 0x00, 0x00, 0x01}
	req.SetISID(isid)

	resp := ln.HandleLoginPDU(req, nil)
	if resp.LoginStatusClass() != LoginStatusInitiatorErr {
		t.Fatalf("expected error for invalid CSG, got %d", resp.LoginStatusClass())
	}
}
