package testrunner

// AuthTokenHeader is the HTTP header used for coordinator↔agent authentication.
const AuthTokenHeader = "X-Auth-Token"

// UploadBasePath is the required prefix for all upload paths.
const UploadBasePath = "/tmp/sw-test-runner/"

// RegisterRequest is sent by an agent to the coordinator at POST /register.
type RegisterRequest struct {
	AgentID      string   `json:"agent_id"`
	Hostname     string   `json:"hostname"`
	IP           string   `json:"ip"`
	Port         int      `json:"port"`
	Nodes        []string `json:"nodes"`
	Capabilities []string `json:"capabilities"`
}

// RegisterResponse is returned by the coordinator after successful registration.
type RegisterResponse struct {
	OK          bool   `json:"ok"`
	AgentIndex  int    `json:"agent_index"`
	TotalAgents int    `json:"total_agents"`
	Error       string `json:"error,omitempty"`
}

// PhaseRequest is sent by the coordinator to an agent at POST /phase.
type PhaseRequest struct {
	PhaseIndex int               `json:"phase_index"`
	PhaseName  string            `json:"phase_name"`
	Actions    []Action          `json:"actions"`
	GlobalVars map[string]string `json:"global_vars"`
	Scenario   *Scenario         `json:"scenario,omitempty"`
}

// PhaseResponse is returned by the agent after executing a phase.
type PhaseResponse struct {
	PhaseIndex int               `json:"phase_index"`
	Results    []ActionResult    `json:"results"`
	NewVars    map[string]string `json:"new_vars"`
	Error      string            `json:"error,omitempty"`
}

// ExecRequest is sent by the coordinator to an agent at POST /exec.
type ExecRequest struct {
	Cmd  string `json:"cmd"`
	Root bool   `json:"root"`
}

// ExecResponse is returned by the agent after executing a command.
type ExecResponse struct {
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	ExitCode int    `json:"exit_code"`
	Error    string `json:"error,omitempty"`
}

// UploadResponse is returned by the agent after receiving a file upload.
type UploadResponse struct {
	OK    bool   `json:"ok"`
	Size  int64  `json:"size"`
	Path  string `json:"path"`
	Error string `json:"error,omitempty"`
}

// HealthResponse is returned by the agent at GET /health.
type HealthResponse struct {
	OK       bool   `json:"ok"`
	AgentID  string `json:"agent_id"`
	UptimeS  int64  `json:"uptime_s"`
	HasRoot  bool   `json:"has_root"`
	Hostname string `json:"hostname"`
}
