package scaffold

import _ "embed"

//go:embed filer.toml
var Filer string

//go:embed notification.toml
var Notification string

//go:embed replication.toml
var Replication string

//go:embed security.toml
var Security string

//go:embed master.toml
var Master string

//go:embed shell.toml
var Shell string
