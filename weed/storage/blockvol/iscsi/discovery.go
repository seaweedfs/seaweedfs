package iscsi

// HandleTextRequest processes a Text Request PDU.
// Currently supports SendTargets discovery (RFC 7143, Section 12.3).
func HandleTextRequest(req *PDU, targets []DiscoveryTarget) *PDU {
	resp := &PDU{}
	resp.SetOpcode(OpTextResp)
	resp.SetOpSpecific1(FlagF) // Final
	resp.SetInitiatorTaskTag(req.InitiatorTaskTag())
	resp.SetTargetTransferTag(0xFFFFFFFF) // no continuation

	params, err := ParseParams(req.DataSegment)
	if err != nil {
		// Malformed — return empty response
		return resp
	}

	val, ok := params.Get("SendTargets")
	if !ok {
		return resp
	}

	// Discovery responses can have duplicate keys (TargetName appears
	// for each target), so we use EncodeDiscoveryTargets directly.
	var matched []DiscoveryTarget

	switch val {
	case "All":
		matched = targets
	default:
		for _, tgt := range targets {
			if tgt.Name == val {
				matched = append(matched, tgt)
				break
			}
		}
	}

	if data := EncodeDiscoveryTargets(matched); len(data) > 0 {
		resp.DataSegment = data
	}
	return resp
}

// DiscoveryTarget represents a target available for discovery.
type DiscoveryTarget struct {
	Name    string // IQN, e.g., "iqn.2024.com.seaweedfs:vol1"
	Address string // IP:port,portal-group, e.g., "10.0.0.1:3260,1"
}

// EncodeDiscoveryTargets encodes multiple targets into text parameter format.
// Each target produces TargetName=<iqn>\0TargetAddress=<addr>\0
// This handles the special multi-value encoding where the same key can appear
// multiple times (unlike normal negotiation params).
func EncodeDiscoveryTargets(targets []DiscoveryTarget) []byte {
	if len(targets) == 0 {
		return nil
	}

	var buf []byte
	for _, tgt := range targets {
		buf = append(buf, "TargetName="+tgt.Name+"\x00"...)
		if tgt.Address != "" {
			buf = append(buf, "TargetAddress="+tgt.Address+"\x00"...)
		}
	}
	return buf
}
