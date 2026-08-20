package plugin

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func intValue(v int64) *plugin_pb.ConfigValue {
	return &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: v}}
}

func stringValue(v string) *plugin_pb.ConfigValue {
	return &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_StringValue{StringValue: v}}
}

// An observation exists to be rendered, so the ConfigValue envelope comes off
// on the way in. A page asking for "fragments" wants 16, not a wrapper holding
// the string "16".
func TestObservationAttributesAreFlattened(t *testing.T) {
	store := NewObservationStore()
	store.Record("worker-1", &plugin_pb.WorkerObservations{
		JobType: "lance_compact",
		Observations: []*plugin_pb.ObjectObservation{{
			ObjectId:   []string{"bucket", "ns", "table"},
			ObjectKind: "table",
			Format:     "LANCE",
			Attributes: map[string]*plugin_pb.ConfigValue{
				"fragments": intValue(16),
				"schema":    stringValue(`[{"name":"id"}]`),
			},
			ObservedAt: timestamppb.New(time.Now()),
		}},
	})

	observed, ok := store.Get([]string{"bucket", "ns", "table"})
	if !ok {
		t.Fatal("observation was not stored")
	}
	if got, ok := observed.Attributes["fragments"].(int64); !ok || got != 16 {
		t.Fatalf("fragments = %#v, want int64 16", observed.Attributes["fragments"])
	}
	if got := observed.AttributeString("fragments"); got != "16" {
		t.Fatalf("AttributeString(fragments) = %q, want \"16\"", got)
	}
	if got := observed.AttributeString("schema"); got != `[{"name":"id"}]` {
		t.Fatalf("AttributeString(schema) = %q", got)
	}
	if observed.WorkerID != "worker-1" || observed.JobType != "lance_compact" {
		t.Fatalf("provenance lost: %+v", observed)
	}
}

// The newest look at an object is the useful one, whichever worker took it.
func TestObservationIsReplacedByALaterOne(t *testing.T) {
	store := NewObservationStore()
	record := func(worker string, fragments int64) {
		store.Record(worker, &plugin_pb.WorkerObservations{
			JobType: "lance_compact",
			Observations: []*plugin_pb.ObjectObservation{{
				ObjectId:   []string{"bucket", "ns", "table"},
				Format:     "LANCE",
				Attributes: map[string]*plugin_pb.ConfigValue{"fragments": intValue(fragments)},
				ObservedAt: timestamppb.New(time.Now()),
			}},
		})
	}
	record("worker-1", 16)
	record("worker-2", 1)

	observed, _ := store.Get([]string{"bucket", "ns", "table"})
	if got := observed.AttributeString("fragments"); got != "1" {
		t.Fatalf("fragments = %q, want the later observation's 1", got)
	}
	if observed.WorkerID != "worker-2" {
		t.Fatalf("worker = %q, want worker-2", observed.WorkerID)
	}
	if len(store.List()) != 1 {
		t.Fatalf("store holds %d entries, want 1", len(store.List()))
	}
}

// Missing attributes read as empty rather than panicking a template.
func TestObservationAttributeStringHandlesAbsence(t *testing.T) {
	var absent *Observation
	if got := absent.AttributeString("fragments"); got != "" {
		t.Fatalf("nil observation returned %q", got)
	}
	store := NewObservationStore()
	store.Record("worker-1", &plugin_pb.WorkerObservations{
		Observations: []*plugin_pb.ObjectObservation{{
			ObjectId: []string{"b", "n", "t"},
		}},
	})
	observed, _ := store.Get([]string{"b", "n", "t"})
	if got := observed.AttributeString("nothing"); got != "" {
		t.Fatalf("absent attribute returned %q", got)
	}
}

// A table path can be dropped and remade in another format. The observation left
// behind describes something that is no longer there, so a caller asking about
// the new format must not be handed it.
func TestObservationLookupIsScopedToFormat(t *testing.T) {
	store := NewObservationStore()
	objectID := []string{"bucket", "ns", "table"}
	store.Record("worker-1", &plugin_pb.WorkerObservations{
		Observations: []*plugin_pb.ObjectObservation{{
			ObjectId:   objectID,
			Format:     "LANCE",
			ObservedAt: timestamppb.New(time.Now()),
		}},
	})

	if _, ok := store.GetFormat(objectID, "ICEBERG"); ok {
		t.Fatal("a LANCE observation was returned for an ICEBERG table")
	}
	if _, ok := store.GetFormat(objectID, "lance"); !ok {
		t.Fatal("format matching must not depend on case")
	}
	if _, ok := store.GetFormat(objectID, ""); ok {
		t.Fatal("an unknown format must not match a recorded one")
	}
}
