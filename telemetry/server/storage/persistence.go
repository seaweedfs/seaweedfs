package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// persistedState is the on-disk snapshot of the in-memory instance map.
type persistedState struct {
	Instances map[string]*telemetryData  `json:"instances"`
	Histories map[string][]HistorySample `json:"histories,omitempty"`
}

// LoadState restores the instance map and Prometheus gauges from a state file
// written by SaveStateIfDirty. A missing file is not an error. Original
// ReceivedAt timestamps are preserved so cleanup and the active-cluster
// windows stay correct across restarts.
func (s *PrometheusStorage) LoadState(path string) (int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	var state persistedState
	if err := json.Unmarshal(b, &state); err != nil {
		return 0, fmt.Errorf("parse %s: %w", path, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	loaded := 0
	for id, instance := range state.Instances {
		if instance == nil || instance.TelemetryData == nil || instance.TelemetryData.TopologyId == "" {
			continue
		}
		s.instances[id] = instance
		s.setClusterMetrics(instance.TelemetryData)
		loaded++
	}
	for id, h := range state.Histories {
		if _, ok := s.instances[id]; ok && len(h) > 0 {
			s.histories[id] = h
		}
	}
	s.updateStats()
	return loaded, nil
}

// SaveStateIfDirty writes the instance map to path if it changed since the
// last successful save. The write is atomic (temp file + rename).
func (s *PrometheusStorage) SaveStateIfDirty(path string) error {
	s.mu.Lock()
	if !s.dirty {
		s.mu.Unlock()
		return nil
	}
	b, err := json.Marshal(&persistedState{Instances: s.instances, Histories: s.histories})
	if err != nil {
		s.mu.Unlock()
		return err
	}
	s.dirty = false
	s.mu.Unlock()

	if err := s.writeAtomically(path, b); err != nil {
		s.mu.Lock()
		s.dirty = true
		s.mu.Unlock()
		return err
	}
	return nil
}

func (s *PrometheusStorage) writeAtomically(path string, b []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), path)
}
