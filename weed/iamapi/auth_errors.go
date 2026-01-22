package iamapi

import (
	"errors"
	"fmt"
)

// Authentication error types for proper HTTP status code mapping

// MalformedRequestError indicates the request has invalid syntax or structure
type MalformedRequestError struct {
	Message string
}

func (e *MalformedRequestError) Error() string {
	return fmt.Sprintf("malformed request: %s", e.Message)
}

// AuthenticationError indicates invalid credentials or signature verification failure
type AuthenticationError struct {
	Message string
}

func (e *AuthenticationError) Error() string {
	return fmt.Sprintf("authentication failed: %s", e.Message)
}

// AuthorizationError indicates the user lacks required permissions
type AuthorizationError struct {
	Message string
}

func (e *AuthorizationError) Error() string {
	return fmt.Sprintf("authorization failed: %s", e.Message)
}

// IsMalformedRequestError checks if an error is a MalformedRequestError
func IsMalformedRequestError(err error) bool {
	var malformedErr *MalformedRequestError
	return errors.As(err, &malformedErr)
}

// IsAuthenticationError checks if an error is an AuthenticationError
func IsAuthenticationError(err error) bool {
	var authErr *AuthenticationError
	return errors.As(err, &authErr)
}

// IsAuthorizationError checks if an error is an AuthorizationError
func IsAuthorizationError(err error) bool {
	var authzErr *AuthorizationError
	return errors.As(err, &authzErr)
}
