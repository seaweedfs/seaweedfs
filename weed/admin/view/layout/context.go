package layout

import "net/http"

// ViewContext contains per-request metadata needed by layout templates.
type ViewContext struct {
	Request   *http.Request
	Username  string
	CSRFToken string
}

// NewViewContext builds a ViewContext from request metadata.
func NewViewContext(r *http.Request, username, csrfToken string) ViewContext {
	return ViewContext{
		Request:   r,
		Username:  username,
		CSRFToken: csrfToken,
	}
}
