package sts

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
)

// MemorySessionStore implements SessionStore using in-memory storage
type MemorySessionStore struct {
	sessions map[string]*SessionInfo
	mutex    sync.RWMutex
}

// NewMemorySessionStore creates a new memory-based session store
func NewMemorySessionStore() *MemorySessionStore {
	return &MemorySessionStore{
		sessions: make(map[string]*SessionInfo),
	}
}

// StoreSession stores session information in memory
func (m *MemorySessionStore) StoreSession(ctx context.Context, sessionId string, session *SessionInfo) error {
	if sessionId == "" {
		return fmt.Errorf("session ID cannot be empty")
	}

	if session == nil {
		return fmt.Errorf("session cannot be nil")
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.sessions[sessionId] = session
	return nil
}

// GetSession retrieves session information from memory
func (m *MemorySessionStore) GetSession(ctx context.Context, sessionId string) (*SessionInfo, error) {
	if sessionId == "" {
		return nil, fmt.Errorf("session ID cannot be empty")
	}

	m.mutex.RLock()
	defer m.mutex.RUnlock()

	session, exists := m.sessions[sessionId]
	if !exists {
		return nil, fmt.Errorf("session not found")
	}

	// Check if session has expired
	if time.Now().After(session.ExpiresAt) {
		return nil, fmt.Errorf("session has expired")
	}

	return session, nil
}

// RevokeSession revokes a session from memory
func (m *MemorySessionStore) RevokeSession(ctx context.Context, sessionId string) error {
	if sessionId == "" {
		return fmt.Errorf("session ID cannot be empty")
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	delete(m.sessions, sessionId)
	return nil
}

// CleanupExpiredSessions removes expired sessions from memory
func (m *MemorySessionStore) CleanupExpiredSessions(ctx context.Context) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	now := time.Now()
	for sessionId, session := range m.sessions {
		if now.After(session.ExpiresAt) {
			delete(m.sessions, sessionId)
		}
	}

	return nil
}

// ExpireSessionForTesting manually expires a session for testing purposes
func (m *MemorySessionStore) ExpireSessionForTesting(ctx context.Context, sessionId string) error {
	if sessionId == "" {
		return fmt.Errorf("session ID cannot be empty")
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	session, exists := m.sessions[sessionId]
	if !exists {
		return fmt.Errorf("session not found")
	}

	// Set expiration to 1 minute in the past to ensure it's expired
	session.ExpiresAt = time.Now().Add(-1 * time.Minute)
	m.sessions[sessionId] = session

	return nil
}

// FilerSessionStore implements SessionStore using SeaweedFS filer
type FilerSessionStore struct {
	filerGrpcAddress string
	grpcDialOption   grpc.DialOption
	basePath         string
}

// NewFilerSessionStore creates a new filer-based session store
func NewFilerSessionStore(config map[string]interface{}) (*FilerSessionStore, error) {
	store := &FilerSessionStore{
		basePath: "/seaweedfs/iam/sessions", // Default path for session storage
	}

	// Parse configuration
	if config != nil {
		if filerAddr, ok := config["filerAddress"].(string); ok {
			store.filerGrpcAddress = filerAddr
		}
		if basePath, ok := config["basePath"].(string); ok {
			store.basePath = strings.TrimSuffix(basePath, "/")
		}
	}

	// Validate configuration
	if store.filerGrpcAddress == "" {
		return nil, fmt.Errorf("filer address is required for FilerSessionStore")
	}

	glog.V(2).Infof("Initialized FilerSessionStore with filer %s, basePath %s",
		store.filerGrpcAddress, store.basePath)

	return store, nil
}

// StoreSession stores session information in filer
func (f *FilerSessionStore) StoreSession(ctx context.Context, sessionId string, session *SessionInfo) error {
	if sessionId == "" {
		return fmt.Errorf("session ID cannot be empty")
	}
	if session == nil {
		return fmt.Errorf("session cannot be nil")
	}

	// Serialize session to JSON
	sessionData, err := json.Marshal(session)
	if err != nil {
		return fmt.Errorf("failed to serialize session: %v", err)
	}

	sessionPath := f.getSessionPath(sessionId)

	// Store in filer
	return f.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.CreateEntryRequest{
			Directory: f.basePath,
			Entry: &filer_pb.Entry{
				Name:        f.getSessionFileName(sessionId),
				IsDirectory: false,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    time.Now().Unix(),
					Crtime:   time.Now().Unix(),
					FileMode: uint32(0600), // Read/write for owner only
					Uid:      uint32(0),
					Gid:      uint32(0),
				},
				Content: sessionData,
			},
		}

		glog.V(3).Infof("Storing session %s at %s", sessionId, sessionPath)
		_, err := client.CreateEntry(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to store session %s: %v", sessionId, err)
		}

		return nil
	})
}

// GetSession retrieves session information from filer
func (f *FilerSessionStore) GetSession(ctx context.Context, sessionId string) (*SessionInfo, error) {
	if sessionId == "" {
		return nil, fmt.Errorf("session ID cannot be empty")
	}

	var sessionData []byte
	err := f.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: f.basePath,
			Name:      f.getSessionFileName(sessionId),
		}

		glog.V(3).Infof("Looking up session %s", sessionId)
		response, err := client.LookupDirectoryEntry(ctx, request)
		if err != nil {
			return fmt.Errorf("session not found: %v", err)
		}

		if response.Entry == nil {
			return fmt.Errorf("session not found")
		}

		sessionData = response.Entry.Content
		return nil
	})

	if err != nil {
		return nil, err
	}

	// Deserialize session from JSON
	var session SessionInfo
	if err := json.Unmarshal(sessionData, &session); err != nil {
		return nil, fmt.Errorf("failed to deserialize session: %v", err)
	}

	// Check if session has expired
	if time.Now().After(session.ExpiresAt) {
		// Clean up expired session
		_ = f.RevokeSession(ctx, sessionId)
		return nil, fmt.Errorf("session has expired")
	}

	return &session, nil
}

// RevokeSession revokes a session from filer
func (f *FilerSessionStore) RevokeSession(ctx context.Context, sessionId string) error {
	if sessionId == "" {
		return fmt.Errorf("session ID cannot be empty")
	}

	return f.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.DeleteEntryRequest{
			Directory:            f.basePath,
			Name:                 f.getSessionFileName(sessionId),
			IsDeleteData:         true,
			IsRecursive:          false,
			IgnoreRecursiveError: false,
		}

		glog.V(3).Infof("Revoking session %s", sessionId)
		resp, err := client.DeleteEntry(ctx, request)
		if err != nil {
			// Ignore "not found" errors - session may already be deleted
			if strings.Contains(err.Error(), "not found") {
				return nil
			}
			return fmt.Errorf("failed to revoke session %s: %v", sessionId, err)
		}

		// Check response error
		if resp.Error != "" {
			// Ignore "not found" errors - session may already be deleted
			if strings.Contains(resp.Error, "not found") {
				return nil
			}
			return fmt.Errorf("failed to revoke session %s: %s", sessionId, resp.Error)
		}

		return nil
	})
}

// CleanupExpiredSessions removes expired sessions from filer
func (f *FilerSessionStore) CleanupExpiredSessions(ctx context.Context) error {
	now := time.Now()
	expiredCount := 0

	err := f.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// List all entries in the session directory
		request := &filer_pb.ListEntriesRequest{
			Directory:          f.basePath,
			Prefix:             "session_",
			StartFromFileName:  "",
			InclusiveStartFrom: false,
			Limit:              1000, // Process in batches of 1000
		}

		stream, err := client.ListEntries(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to list sessions: %v", err)
		}

		for {
			resp, err := stream.Recv()
			if err != nil {
				break // End of stream or error
			}

			if resp.Entry == nil || resp.Entry.IsDirectory {
				continue
			}

			// Parse session data to check expiration
			var session SessionInfo
			if err := json.Unmarshal(resp.Entry.Content, &session); err != nil {
				glog.V(2).Infof("Failed to parse session file %s, deleting: %v", resp.Entry.Name, err)
				// Delete corrupted session file
				f.deleteSessionFile(ctx, client, resp.Entry.Name)
				continue
			}

			// Check if session is expired
			if now.After(session.ExpiresAt) {
				glog.V(3).Infof("Cleaning up expired session: %s", resp.Entry.Name)
				if err := f.deleteSessionFile(ctx, client, resp.Entry.Name); err != nil {
					glog.V(1).Infof("Failed to delete expired session %s: %v", resp.Entry.Name, err)
				} else {
					expiredCount++
				}
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	if expiredCount > 0 {
		glog.V(2).Infof("Cleaned up %d expired sessions", expiredCount)
	}

	return nil
}

// Helper methods

// SetFilerClient sets the filer client connection details
func (f *FilerSessionStore) SetFilerClient(filerAddress string, grpcDialOption grpc.DialOption) {
	f.filerGrpcAddress = filerAddress
	f.grpcDialOption = grpcDialOption
}

// withFilerClient executes a function with a filer client
func (f *FilerSessionStore) withFilerClient(fn func(client filer_pb.SeaweedFilerClient) error) error {
	if f.filerGrpcAddress == "" {
		return fmt.Errorf("filer address not configured")
	}

	// Use the pb.WithGrpcFilerClient helper similar to existing SeaweedFS code
	return pb.WithGrpcFilerClient(false, 0, pb.ServerAddress(f.filerGrpcAddress), f.grpcDialOption, fn)
}

// getSessionPath returns the full path for a session
func (f *FilerSessionStore) getSessionPath(sessionId string) string {
	return f.basePath + "/" + f.getSessionFileName(sessionId)
}

// getSessionFileName returns the filename for a session
func (f *FilerSessionStore) getSessionFileName(sessionId string) string {
	return "session_" + sessionId + ".json"
}

// deleteSessionFile deletes a session file
func (f *FilerSessionStore) deleteSessionFile(ctx context.Context, client filer_pb.SeaweedFilerClient, fileName string) error {
	request := &filer_pb.DeleteEntryRequest{
		Directory:            f.basePath,
		Name:                 fileName,
		IsDeleteData:         true,
		IsRecursive:          false,
		IgnoreRecursiveError: false,
	}

	_, err := client.DeleteEntry(ctx, request)
	return err
}
