package plugin

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// maxObservations caps the store. Observations are a convenience for display,
// so a cluster with more objects than this loses the oldest rather than growing
// admin's memory without limit.
const maxObservations = 10000

// observationKey identifies one observed object across workers.
func observationKey(objectID []string) string {
	return strings.Join(objectID, "\x1f")
}

// ObservationStore keeps the last thing a worker said about each object.
//
// It is deliberately not authoritative: a worker reports what it saw when it
// last looked, and admin serves that back with its timestamp so a reader can
// judge how stale it is. Nothing schedules work from it.
type ObservationStore struct {
	mu      sync.RWMutex
	entries map[string]*Observation
}

// Observation is one object as a worker last reported it.
type Observation struct {
	ObjectID   []string `json:"object_id"`
	ObjectKind string   `json:"object_kind"`
	Format     string   `json:"format"`
	// Attributes are flattened on the way in: they exist to be displayed and
	// served as JSON, and the typed form is the worker's business.
	Attributes map[string]interface{} `json:"attributes"`
	JobType    string                 `json:"job_type"`
	WorkerID   string                 `json:"worker_id"`
	ObservedAt time.Time              `json:"observed_at"`
}

func NewObservationStore() *ObservationStore {
	return &ObservationStore{entries: make(map[string]*Observation)}
}

// Record stores what one worker reported, replacing whatever was there for the
// same object. A later observation from a different worker still wins: the
// newest look at an object is the useful one.
func (s *ObservationStore) Record(workerID string, report *plugin_pb.WorkerObservations) {
	if report == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, observed := range report.Observations {
		if observed == nil || len(observed.ObjectId) == 0 {
			continue
		}
		at := time.Now()
		if observed.ObservedAt != nil {
			at = observed.ObservedAt.AsTime()
		}
		s.entries[observationKey(observed.ObjectId)] = &Observation{
			ObjectID:   observed.ObjectId,
			ObjectKind: observed.ObjectKind,
			Format:     observed.Format,
			Attributes: flattenAttributes(observed.Attributes),
			JobType:    report.JobType,
			WorkerID:   workerID,
			ObservedAt: at,
		}
	}
	s.evictOldest()
}

// evictOldest trims the store back under the cap. Called with the lock held.
func (s *ObservationStore) evictOldest() {
	if len(s.entries) <= maxObservations {
		return
	}
	keys := make([]string, 0, len(s.entries))
	for key := range s.entries {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return s.entries[keys[i]].ObservedAt.Before(s.entries[keys[j]].ObservedAt)
	})
	for _, key := range keys[:len(s.entries)-maxObservations] {
		delete(s.entries, key)
	}
}

// flattenAttributes unwraps the ConfigValue envelope that configValueMapToPlain
// preserves. An observation exists to be read, and {"int64_value":"16"} is not
// a number anyone wants to render; a protojson int64 also arrives as a string,
// so it is converted back.
func flattenAttributes(values map[string]*plugin_pb.ConfigValue) map[string]interface{} {
	plain := configValueMapToPlain(values)
	if plain == nil {
		return nil
	}
	flat := make(map[string]interface{}, len(plain))
	for name, value := range plain {
		wrapper, ok := value.(map[string]interface{})
		if !ok || len(wrapper) != 1 {
			flat[name] = value
			continue
		}
		for kind, inner := range wrapper {
			if kind == "int64_value" {
				if text, ok := inner.(string); ok {
					if parsed, err := strconv.ParseInt(text, 10, 64); err == nil {
						flat[name] = parsed
						continue
					}
				}
			}
			flat[name] = inner
		}
	}
	return flat
}

// Get returns the last observation of one object, if any.
func (s *ObservationStore) Get(objectID []string) (*Observation, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	observed, ok := s.entries[observationKey(objectID)]
	return observed, ok
}

// List returns every observation, newest first.
func (s *ObservationStore) List() []*Observation {
	s.mu.RLock()
	defer s.mu.RUnlock()
	all := make([]*Observation, 0, len(s.entries))
	for _, observed := range s.entries {
		all = append(all, observed)
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i].ObservedAt.After(all[j].ObservedAt)
	})
	return all
}

// AttributeString renders one attribute for display, or "" when it is absent.
func (o *Observation) AttributeString(name string) string {
	if o == nil {
		return ""
	}
	value, ok := o.Attributes[name]
	if !ok || value == nil {
		return ""
	}
	return fmt.Sprintf("%v", value)
}

// Observations exposes the store so admin handlers can serve it.
func (r *Plugin) Observations() *ObservationStore {
	return r.observations
}
