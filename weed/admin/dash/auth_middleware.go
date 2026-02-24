package dash

import (
	"crypto/subtle"
	"net/http"

	"github.com/gorilla/sessions"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// ShowLogin displays the login page.
func (s *AdminServer) ShowLogin(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

// HandleLogin handles login form submission.
func (s *AdminServer) HandleLogin(store sessions.Store, adminUser, adminPassword, readOnlyUser, readOnlyPassword string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Redirect(w, r, "/login?error=Invalid form submission", http.StatusSeeOther)
			return
		}
		loginUsername := r.FormValue("username")
		loginPassword := r.FormValue("password")

		var role string
		var authenticated bool

		// Check admin credentials.
		if adminPassword != "" && loginUsername == adminUser && subtle.ConstantTimeCompare([]byte(loginPassword), []byte(adminPassword)) == 1 {
			role = "admin"
			authenticated = true
		} else if readOnlyPassword != "" && loginUsername == readOnlyUser && subtle.ConstantTimeCompare([]byte(loginPassword), []byte(readOnlyPassword)) == 1 {
			// Check read-only credentials.
			role = "readonly"
			authenticated = true
		}

		if authenticated {
			session, err := store.Get(r, sessionName)
			if err != nil {
				http.Redirect(w, r, "/login?error=Unable to create session. Please try again or contact administrator.", http.StatusSeeOther)
				return
			}
			for key := range session.Values {
				delete(session.Values, key)
			}
			session.Values["authenticated"] = true
			session.Values["username"] = loginUsername
			session.Values["role"] = role
			csrfToken, err := generateCSRFToken()
			if err != nil {
				http.Redirect(w, r, "/login?error=Unable to create session. Please try again or contact administrator.", http.StatusSeeOther)
				return
			}
			session.Values[sessionCSRFTokenKey] = csrfToken
			if err := session.Save(r, w); err != nil {
				// Log the detailed error server-side for diagnostics.
				glog.Errorf("Failed to save session for user %s: %v", loginUsername, err)
				http.Redirect(w, r, "/login?error=Unable to create session. Please try again or contact administrator.", http.StatusSeeOther)
				return
			}

			http.Redirect(w, r, "/admin", http.StatusSeeOther)
			return
		}

		// Authentication failed.
		http.Redirect(w, r, "/login?error=Invalid credentials", http.StatusSeeOther)
	}
}

// HandleLogout handles user logout.
func (s *AdminServer) HandleLogout(store sessions.Store, w http.ResponseWriter, r *http.Request) {
	session, err := store.Get(r, sessionName)
	if err != nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	for key := range session.Values {
		delete(session.Values, key)
	}
	session.Options.MaxAge = -1
	if err := session.Save(r, w); err != nil {
		glog.Warningf("Failed to save session during logout: %v", err)
	}
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}
