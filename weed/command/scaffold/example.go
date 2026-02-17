package scaffold

import (
	_ "embed"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/cluster/maintenance"
)

func init() {
	Master = strings.ReplaceAll(masterTemplate, "{{DEFAULT_MAINTENANCE_SCRIPTS}}", maintenance.DefaultMasterMaintenanceScripts)
}

//go:embed filer.toml
var Filer string

//go:embed notification.toml
var Notification string

//go:embed replication.toml
var Replication string

//go:embed security.toml
var Security string

//go:embed master.toml
var masterTemplate string
var Master string

//go:embed shell.toml
var Shell string

//go:embed credential.toml
var Credential string
