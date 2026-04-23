package user

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

func newTempStore(t *testing.T) *FileStore {
	t.Helper()
	dir := t.TempDir()
	s, err := NewFileStore(filepath.Join(dir, "users.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	return s
}

func readUsers(t *testing.T, s *FileStore) []*User {
	t.Helper()
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		t.Fatalf("read store file: %v", err)
	}
	var users []*User
	if err := json.Unmarshal(data, &users); err != nil {
		t.Fatalf("unmarshal store file: %v", err)
	}
	return users
}

func TestCreateUserHashesPassword(t *testing.T) {
	s := newTempStore(t)
	u, err := s.CreateUser("alice", "s3cret")
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if u.Password != "" {
		t.Errorf("plaintext Password leaked: %q", u.Password)
	}
	if u.HashedPassword == "" {
		t.Fatal("HashedPassword empty")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(u.HashedPassword), []byte("s3cret")); err != nil {
		t.Fatalf("hash does not match password: %v", err)
	}
	stored := readUsers(t, s)
	if len(stored) != 1 || stored[0].Password != "" {
		t.Errorf("plaintext persisted: %+v", stored)
	}
}

func TestValidatePasswordMigratesLegacyPlaintext(t *testing.T) {
	s := newTempStore(t)
	legacy := NewUser("bob")
	legacy.Password = "hunter2"
	if err := s.SaveUser(legacy); err != nil {
		t.Fatalf("SaveUser: %v", err)
	}

	if !s.ValidatePassword("bob", []byte("hunter2")) {
		t.Fatal("legacy login rejected")
	}
	u, err := s.GetUser("bob")
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if u.Password != "" {
		t.Errorf("legacy Password not cleared: %q", u.Password)
	}
	if u.HashedPassword == "" {
		t.Fatal("HashedPassword not populated after migration")
	}

	stored := readUsers(t, s)
	if len(stored) != 1 || stored[0].Password != "" || stored[0].HashedPassword == "" {
		t.Errorf("migration not persisted: %+v", stored)
	}
}

// Successful logins that don't migrate must not rewrite the store file.
// We detect a rewrite by replacing the file contents with a sentinel after
// CreateUser and verifying the sentinel is untouched after a valid login.
func TestValidatePasswordNoRewriteWithoutMigration(t *testing.T) {
	s := newTempStore(t)
	if _, err := s.CreateUser("carol", "pw"); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	sentinel := []byte("SENTINEL_NOT_REAL_JSON")
	if err := os.WriteFile(s.filePath, sentinel, 0600); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}

	if !s.ValidatePassword("carol", []byte("pw")) {
		t.Fatal("login rejected")
	}
	got, err := os.ReadFile(s.filePath)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, sentinel) {
		t.Errorf("store file was rewritten on non-migrating login: got %q", got)
	}
}

func TestValidatePasswordWrongPassword(t *testing.T) {
	s := newTempStore(t)
	if _, err := s.CreateUser("dave", "correct"); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if s.ValidatePassword("dave", []byte("wrong")) {
		t.Error("wrong password accepted for bcrypt user")
	}

	legacy := NewUser("eve")
	legacy.Password = "plain"
	if err := s.SaveUser(legacy); err != nil {
		t.Fatal(err)
	}
	if s.ValidatePassword("eve", []byte("wrong")) {
		t.Error("wrong password accepted for legacy user")
	}
	if s.ValidatePassword("eve", []byte("")) {
		t.Error("empty password accepted for legacy user")
	}
}

func TestValidatePasswordUnknownUser(t *testing.T) {
	s := newTempStore(t)
	if s.ValidatePassword("nobody", []byte("whatever")) {
		t.Error("unknown user accepted")
	}
}

func TestCheckPasswordEmptyCredentials(t *testing.T) {
	u := NewUser("frank")
	if ok, _ := u.CheckPassword(""); ok {
		t.Error("empty password accepted on user with no credentials")
	}
	if ok, _ := u.CheckPassword("x"); ok {
		t.Error("password accepted on user with no credentials")
	}
}

func TestSetPasswordTooLong(t *testing.T) {
	u := NewUser("grace")
	if err := u.SetPassword(strings.Repeat("a", 73)); err == nil {
		t.Error("expected error for >72 byte password")
	}
	if u.HashedPassword != "" {
		t.Errorf("HashedPassword set despite error: %q", u.HashedPassword)
	}
}

// Concurrent ValidatePassword calls on a legacy user must not race on the
// User fields and must converge to a single persisted bcrypt hash.
func TestValidatePasswordConcurrentMigration(t *testing.T) {
	s := newTempStore(t)
	legacy := NewUser("heidi")
	legacy.Password = "p"
	if err := s.SaveUser(legacy); err != nil {
		t.Fatal(err)
	}

	const N = 16
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			if !s.ValidatePassword("heidi", []byte("p")) {
				t.Error("login rejected")
			}
		}()
	}
	wg.Wait()

	u, err := s.GetUser("heidi")
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if u.Password != "" {
		t.Errorf("plaintext not cleared after concurrent migration: %q", u.Password)
	}
	if err := bcrypt.CompareHashAndPassword([]byte(u.HashedPassword), []byte("p")); err != nil {
		t.Errorf("final hash invalid: %v", err)
	}
}
