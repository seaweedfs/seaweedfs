package webhook

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

// ManagedTarget is a webhook target kept in the filer and edited from the
// admin UI. Zero numeric settings take the same defaults as notification.toml.
type ManagedTarget struct {
	Name              string   `json:"name"`
	Enabled           bool     `json:"enabled"`
	Endpoint          string   `json:"endpoint"`
	BearerToken       string   `json:"bearer_token,omitempty"`
	EventTypes        []string `json:"event_types,omitempty"`
	PathPrefixes      []string `json:"path_prefixes,omitempty"`
	TimeoutSeconds    int      `json:"timeout_seconds,omitempty"`
	MaxRetries        int      `json:"max_retries,omitempty"`
	BackoffSeconds    int      `json:"backoff_seconds,omitempty"`
	MaxBackoffSeconds int      `json:"max_backoff_seconds,omitempty"`
	Workers           int      `json:"workers,omitempty"`
	BufferSize        int      `json:"buffer_size,omitempty"`
}

type ManagedConfig struct {
	Targets []ManagedTarget `json:"targets"`
}

var managedTargetName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

func ParseManagedConfig(data []byte) (*ManagedConfig, error) {
	c := &ManagedConfig{}
	if len(strings.TrimSpace(string(data))) == 0 {
		return c, nil
	}
	if err := json.Unmarshal(data, c); err != nil {
		return nil, fmt.Errorf("parse webhook targets: %w", err)
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *ManagedConfig) Validate() error {
	seen := make(map[string]bool, len(c.Targets))
	for i := range c.Targets {
		t := &c.Targets[i]
		if seen[t.Name] {
			return fmt.Errorf("duplicate webhook target %q", t.Name)
		}
		seen[t.Name] = true
		if err := t.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (t *ManagedTarget) Validate() error {
	if !managedTargetName.MatchString(t.Name) {
		return fmt.Errorf("webhook target name %q must be 1-64 letters, digits, '-' or '_'", t.Name)
	}
	u, err := url.Parse(t.Endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("webhook target %q: endpoint must be an http or https URL", t.Name)
	}
	for _, et := range t.EventTypes {
		if !eventType(et).valid() {
			return fmt.Errorf("webhook target %q: invalid event type %q", t.Name, et)
		}
	}
	for _, p := range t.PathPrefixes {
		if !strings.HasPrefix(p, "/") {
			return fmt.Errorf("webhook target %q: path prefix %q must start with '/'", t.Name, p)
		}
	}
	for field, v := range t.settings() {
		if v < 0 {
			return fmt.Errorf("webhook target %q: %s must not be negative", t.Name, field)
		}
	}
	if err := t.config().validate(); err != nil {
		return fmt.Errorf("webhook target %q: %w", t.Name, err)
	}
	return nil
}

func (t *ManagedTarget) settings() map[string]int {
	return map[string]int{
		"timeout_seconds":     t.TimeoutSeconds,
		"max_retries":         t.MaxRetries,
		"backoff_seconds":     t.BackoffSeconds,
		"max_backoff_seconds": t.MaxBackoffSeconds,
		"workers":             t.Workers,
		"buffer_size":         t.BufferSize,
	}
}

func (t *ManagedTarget) config() *config {
	c := defaultConfig()
	c.endpoint = t.Endpoint
	c.authBearerToken = t.BearerToken
	c.eventTypes = t.EventTypes
	c.pathPrefixes = t.PathPrefixes
	for dst, v := range map[*int]int{
		&c.timeoutSeconds:    t.TimeoutSeconds,
		&c.maxRetries:        t.MaxRetries,
		&c.backoffSeconds:    t.BackoffSeconds,
		&c.maxBackoffSeconds: t.MaxBackoffSeconds,
		&c.nWorkers:          t.Workers,
		&c.bufferSize:        t.BufferSize,
	} {
		if v > 0 {
			*dst = v
		}
	}
	return c
}

// NewManagedQueue starts delivering to the enabled targets; it returns nil
// when none is enabled.
func NewManagedQueue(c *ManagedConfig) (*Queue, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	var targets []*target
	for i := range c.Targets {
		if !c.Targets[i].Enabled {
			continue
		}
		t, err := newTarget(c.Targets[i].Name, c.Targets[i].config())
		if err != nil {
			return nil, err
		}
		targets = append(targets, t)
	}
	if len(targets) == 0 {
		return nil, nil
	}
	w := &Queue{managed: true}
	if err := w.initialize(targets); err != nil {
		w.cancel()
		return nil, err
	}
	return w, nil
}
