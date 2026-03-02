package handlers

import "github.com/seaweedfs/seaweedfs/weed/admin/dash"

type AuthConfig struct {
	AdminUser     string
	AdminPassword string
	OIDCAuth      *dash.OIDCAuthService
}

func (c AuthConfig) LocalAuthEnabled() bool {
	return c.AdminPassword != ""
}

func (c AuthConfig) OIDCAuthEnabled() bool {
	return c.OIDCAuth != nil
}

func (c AuthConfig) AuthRequired() bool {
	return c.LocalAuthEnabled() || c.OIDCAuthEnabled()
}
