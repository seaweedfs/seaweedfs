package layout

import (
	"net/http"

	"github.com/a-h/templ"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
)

// ViewContext contains per-request metadata needed by layout templates.
type ViewContext struct {
	Request   *http.Request
	Username  string
	CSRFToken string
	URLPrefix string
}

// NewViewContext builds a ViewContext from request metadata.
func NewViewContext(r *http.Request, username, csrfToken string) ViewContext {
	return ViewContext{
		Request:   r,
		Username:  username,
		CSRFToken: csrfToken,
		URLPrefix: dash.URLPrefixFromContext(r.Context()),
	}
}

// P returns the URL prefix prepended to the given path as a templ.SafeURL.
func (v ViewContext) P(path string) templ.SafeURL {
	return templ.SafeURL(v.URLPrefix + path)
}
