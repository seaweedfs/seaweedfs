package dash

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/notification/webhook"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// NotificationWebhook is a stored webhook target as shown to the browser:
// the bearer token never leaves the server.
type NotificationWebhook struct {
	webhook.ManagedTarget
	HasBearerToken bool `json:"has_bearer_token"`
}

type NotificationWebhooksData struct {
	Webhooks   []NotificationWebhook `json:"webhooks"`
	ConfigPath string                `json:"config_path"`
}

// NotificationWebhookRequest creates or replaces a target. An empty
// bearer_token keeps the stored one unless clear_bearer_token is set.
type NotificationWebhookRequest struct {
	webhook.ManagedTarget
	ClearBearerToken bool `json:"clear_bearer_token"`
}

var notificationWebhooksMu sync.Mutex

func (s *AdminServer) readNotificationWebhooks(ctx context.Context) (*webhook.ManagedConfig, error) {
	var conf *webhook.ManagedConfig
	err := s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		var buf bytes.Buffer
		err := filer.ReadEntry(s.masterClient, client, filer.DirectoryEtcSeaweedFS, filer.NotificationWebhooksConfName, &buf)
		if err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
			return err
		}
		conf, err = webhook.ParseManagedConfig(buf.Bytes())
		return err
	})
	return conf, err
}

func (s *AdminServer) saveNotificationWebhooks(ctx context.Context, conf *webhook.ManagedConfig) error {
	if err := conf.Validate(); err != nil {
		return err
	}
	content, err := json.MarshalIndent(conf, "", "  ")
	if err != nil {
		return err
	}
	return s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		return filer.SaveInsideFiler(ctx, client, filer.DirectoryEtcSeaweedFS, filer.NotificationWebhooksConfName, content)
	})
}

func (s *AdminServer) GetNotificationWebhooks(ctx context.Context) (*NotificationWebhooksData, error) {
	conf, err := s.readNotificationWebhooks(ctx)
	if err != nil {
		return nil, err
	}
	data := &NotificationWebhooksData{
		Webhooks:   make([]NotificationWebhook, 0, len(conf.Targets)),
		ConfigPath: filer.NotificationWebhooksConfPath,
	}
	for _, t := range conf.Targets {
		hasToken := t.BearerToken != ""
		t.BearerToken = ""
		data.Webhooks = append(data.Webhooks, NotificationWebhook{ManagedTarget: t, HasBearerToken: hasToken})
	}
	return data, nil
}

func (s *AdminServer) GetNotificationWebhooksAPI(w http.ResponseWriter, r *http.Request) {
	data, err := s.GetNotificationWebhooks(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to read webhook targets: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (s *AdminServer) CreateNotificationWebhookAPI(w http.ResponseWriter, r *http.Request) {
	s.putNotificationWebhook(w, r, "", true)
}

func (s *AdminServer) UpdateNotificationWebhookAPI(w http.ResponseWriter, r *http.Request) {
	s.putNotificationWebhook(w, r, mux.Vars(r)["name"], false)
}

func (s *AdminServer) putNotificationWebhook(w http.ResponseWriter, r *http.Request, name string, create bool) {
	if !requireSessionCSRFToken(w, r) {
		return
	}
	var req NotificationWebhookRequest
	if err := decodeJSONBody(newJSONMaxReader(w, r), &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}
	if create {
		name = req.Name
	} else if req.Name != "" && req.Name != name {
		writeJSONError(w, http.StatusBadRequest, "Webhook target name cannot be changed")
		return
	}
	target := req.ManagedTarget
	target.Name = name

	notificationWebhooksMu.Lock()
	defer notificationWebhooksMu.Unlock()
	conf, err := s.readNotificationWebhooks(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to read webhook targets: "+err.Error())
		return
	}
	index := -1
	for i := range conf.Targets {
		if conf.Targets[i].Name == name {
			index = i
		}
	}
	switch {
	case create && index >= 0:
		writeJSONError(w, http.StatusConflict, "Webhook target already exists")
		return
	case !create && index < 0:
		writeJSONError(w, http.StatusNotFound, "Webhook target not found")
		return
	}
	if index >= 0 && target.BearerToken == "" && !req.ClearBearerToken {
		target.BearerToken = conf.Targets[index].BearerToken
	}
	if err := target.Validate(); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if index >= 0 {
		conf.Targets[index] = target
	} else {
		conf.Targets = append(conf.Targets, target)
	}
	if err := s.saveNotificationWebhooks(r.Context(), conf); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to save webhook targets: "+err.Error())
		return
	}
	status := http.StatusOK
	if create {
		status = http.StatusCreated
	}
	hasToken := target.BearerToken != ""
	target.BearerToken = ""
	writeJSON(w, status, NotificationWebhook{ManagedTarget: target, HasBearerToken: hasToken})
}

func (s *AdminServer) DeleteNotificationWebhookAPI(w http.ResponseWriter, r *http.Request) {
	if !requireSessionCSRFToken(w, r) {
		return
	}
	name := mux.Vars(r)["name"]

	notificationWebhooksMu.Lock()
	defer notificationWebhooksMu.Unlock()
	conf, err := s.readNotificationWebhooks(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to read webhook targets: "+err.Error())
		return
	}
	kept := conf.Targets[:0]
	for _, t := range conf.Targets {
		if t.Name != name {
			kept = append(kept, t)
		}
	}
	if len(kept) == len(conf.Targets) {
		writeJSONError(w, http.StatusNotFound, "Webhook target not found")
		return
	}
	conf.Targets = kept
	if err := s.saveNotificationWebhooks(r.Context(), conf); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to save webhook targets: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"message": "Webhook target deleted"})
}
