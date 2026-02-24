package dash

import "context"

type contextKey string

const (
	contextUsernameKey contextKey = "admin.username"
	contextRoleKey     contextKey = "admin.role"
	contextCSRFKey     contextKey = "admin.csrf"
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
