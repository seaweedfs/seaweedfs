package operation

import "testing"

// An enforced location on a storage option is a placement contract: the assign
// request must carry it, and no widened fallback may follow it.
func TestToAssignRequestsKeepsEnforcedLocation(t *testing.T) {
	so := &StorageOption{
		Collection:      "dump",
		Rack:            "l1000",
		DataCenter:      "dc1",
		EnforceLocation: true,
	}
	ar, alt := so.ToAssignRequests(1)
	if ar.DataCenter != "dc1" || ar.Rack != "l1000" {
		t.Fatalf("expected the rule's location, got dc=%q rack=%q", ar.DataCenter, ar.Rack)
	}
	if alt != nil {
		t.Fatalf("enforced location must not be widened, got alt dc=%q rack=%q", alt.DataCenter, alt.Rack)
	}
}

// A filer-level default location is a preference: the request still carries
// it, and a location-stripped fallback follows so a preferred but unservable
// location does not fail the write.
func TestToAssignRequestsWidensDefaultLocation(t *testing.T) {
	so := &StorageOption{
		Collection: "dump",
		Rack:       "l1000",
		DataCenter: "dc1",
	}
	ar, alt := so.ToAssignRequests(1)
	if ar.DataCenter != "dc1" || ar.Rack != "l1000" {
		t.Fatalf("expected the preferred location, got dc=%q rack=%q", ar.DataCenter, ar.Rack)
	}
	if alt == nil || alt.DataCenter != "" || alt.Rack != "" {
		t.Fatalf("default location should keep a widened fallback, got %+v", alt)
	}
	if alt.Collection != "dump" || alt.Replication != ar.Replication {
		t.Fatalf("fallback must preserve the rest of the option, got %+v", alt)
	}
}
