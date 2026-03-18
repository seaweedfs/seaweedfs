package dash

import (
	"context"

	"github.com/a-h/templ"
)

type contextKey string

const (
	contextUsernameKey  contextKey = "admin.username"
	contextRoleKey      contextKey = "admin.role"
	contextCSRFKey      contextKey = "admin.csrf"
	contextURLPrefixKey contextKey = "admin.urlprefix"
)

// WithAuthContext stores auth metadata on the request context.
func WithAuthContext(ctx context.Context, username, role, csrfToken string) context.Context {
	if username != "" {
		ctx = context.WithValue(ctx, contextUsernameKey, username)
	}
	if role != "" {
		ctx = context.WithValue(ctx, contextRoleKey, role)
	}
	if csrfToken != "" {
		ctx = context.WithValue(ctx, contextCSRFKey, csrfToken)
	}
	return ctx
}

// UsernameFromContext retrieves the username from context.
func UsernameFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if value, ok := ctx.Value(contextUsernameKey).(string); ok {
		return value
	}
	return ""
}

// RoleFromContext retrieves the role from context.
func RoleFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if value, ok := ctx.Value(contextRoleKey).(string); ok {
		return value
	}
	return ""
}

// CSRFTokenFromContext retrieves the CSRF token from context.
func CSRFTokenFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if value, ok := ctx.Value(contextCSRFKey).(string); ok {
		return value
	}
	return ""
}

// WithURLPrefix stores the URL prefix on the context.
func WithURLPrefix(ctx context.Context, prefix string) context.Context {
	return context.WithValue(ctx, contextURLPrefixKey, prefix)
}

// URLPrefixFromContext retrieves the URL prefix from context.
func URLPrefixFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if value, ok := ctx.Value(contextURLPrefixKey).(string); ok {
		return value
	}
	return ""
}

// P returns the URL prefix prepended to the given path.
// Use in Go handlers for redirect URLs.
func P(ctx context.Context, path string) string {
	return URLPrefixFromContext(ctx) + path
}

// PUrl returns the URL prefix prepended to the given path as a templ.SafeURL.
// Use in templ templates for href attributes.
func PUrl(ctx context.Context, path string) templ.SafeURL {
	return templ.SafeURL(URLPrefixFromContext(ctx) + path)
}
