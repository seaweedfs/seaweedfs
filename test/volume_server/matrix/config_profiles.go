package matrix

import "time"

// Profile describes one runtime test matrix configuration.
type Profile struct {
	Name string

	ReadMode        string
	SplitPublicPort bool

	EnableJWT      bool
	JWTSigningKey  string
	JWTReadKey     string
	EnableMaintain bool

	ConcurrentUploadLimitMB   int
	ConcurrentDownloadLimitMB int
	InflightUploadTimeout     time.Duration
	InflightDownloadTimeout   time.Duration

	ReplicatedLayout bool
	HasErasureCoding bool
	HasRemoteTier    bool
}

// P1 is the baseline profile: one volume server, no JWT, proxy read mode.
func P1() Profile {
	return Profile{
		Name:            "P1",
		ReadMode:        "proxy",
		SplitPublicPort: false,
	}
}

// P2 uses split public/admin ports to verify public read-only behavior.
func P2() Profile {
	p := P1()
	p.Name = "P2"
	p.SplitPublicPort = true
	return p
}

// P3 enables JWT verification for read/write flows.
func P3() Profile {
	p := P1()
	p.Name = "P3"
	p.EnableJWT = true
	p.JWTSigningKey = "volume-server-write-key"
	p.JWTReadKey = "volume-server-read-key"
	return p
}

// P8 enables upload/download throttling branches.
func P8() Profile {
	p := P1()
	p.Name = "P8"
	p.ConcurrentUploadLimitMB = 1
	p.ConcurrentDownloadLimitMB = 1
	p.InflightUploadTimeout = 2 * time.Second
	p.InflightDownloadTimeout = 2 * time.Second
	return p
}
