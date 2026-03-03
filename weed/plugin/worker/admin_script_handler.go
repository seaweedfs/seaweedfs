package pluginworker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/shell"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	adminScriptJobType        = "admin_script"
	maxAdminScriptOutputBytes = 16 * 1024
	defaultAdminScriptRunMins = 17
	adminScriptDetectTickSecs = 60
)

const defaultAdminScript = `
ec.balance -apply
fs.log.purge -daysAgo=7
volume.deleteEmpty -quietFor=24h -apply
volume.balance -apply
volume.fix.replication -apply
s3.clean.uploads -timeAgo=24h
`

var adminScriptTokenRegex = regexp.MustCompile(`'.*?'|".*?"|\S+`)

type AdminScriptHandler struct {
	grpcDialOption grpc.DialOption
}

func NewAdminScriptHandler(grpcDialOption grpc.DialOption) *AdminScriptHandler {
	return &AdminScriptHandler{grpcDialOption: grpcDialOption}
}

func (h *AdminScriptHandler) Capability() *plugin_pb.JobTypeCapability {
	return &plugin_pb.JobTypeCapability{
		JobType:                 adminScriptJobType,
		CanDetect:               true,
		CanExecute:              true,
		MaxDetectionConcurrency: 1,
		MaxExecutionConcurrency: 1,
		DisplayName:             "Admin Script",
		Description:             "Execute custom admin shell scripts",
		Weight:                  20,
	}
}

func (h *AdminScriptHandler) Descriptor() *plugin_pb.JobTypeDescriptor {
	return &plugin_pb.JobTypeDescriptor{
		JobType:           adminScriptJobType,
		DisplayName:       "Admin Script",
		Description:       "Run custom admin shell scripts not covered by built-in job types",
		Icon:              "fas fa-terminal",
		DescriptorVersion: 1,
		AdminConfigForm: &plugin_pb.ConfigForm{
			FormId:      "admin-script-admin",
			Title:       "Admin Script Configuration",
			Description: "Define the admin shell script to execute.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "script",
					Title:       "Script",
					Description: "Commands run sequentially by the admin script worker.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "script",
							Label:       "Script",
							Description: "Admin shell commands to execute (one per line).",
							HelpText:    "Lock/unlock are handled by the admin server; omit explicit lock/unlock commands.",
							Placeholder: "volume.balance -apply\nvolume.fix.replication -apply",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXTAREA,
							Required:    true,
						},
						{
							Name:        "run_interval_minutes",
							Label:       "Run Interval (minutes)",
							Description: "Minimum interval between successful admin script runs.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							Required:    true,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
						},
					},
				},
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"script": {
					Kind: &plugin_pb.ConfigValue_StringValue{StringValue: defaultAdminScript},
				},
				"run_interval_minutes": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultAdminScriptRunMins},
				},
			},
		},
		AdminRuntimeDefaults: &plugin_pb.AdminRuntimeDefaults{
			Enabled:                       true,
			DetectionIntervalSeconds:      adminScriptDetectTickSecs,
			DetectionTimeoutSeconds:       300,
			MaxJobsPerDetection:           1,
			GlobalExecutionConcurrency:    1,
			PerWorkerExecutionConcurrency: 1,
			RetryLimit:                    0,
			RetryBackoffSeconds:           30,
		},
		WorkerDefaultValues: map[string]*plugin_pb.ConfigValue{},
	}
}

func (h *AdminScriptHandler) Detect(ctx context.Context, request *plugin_pb.RunDetectionRequest, sender DetectionSender) error {
	if request == nil {
		return fmt.Errorf("run detection request is nil")
	}
	if sender == nil {
		return fmt.Errorf("detection sender is nil")
	}
	if request.JobType != "" && request.JobType != adminScriptJobType {
		return fmt.Errorf("job type %q is not handled by admin_script worker", request.JobType)
	}

	script := normalizeAdminScript(readStringConfig(request.GetAdminConfigValues(), "script", ""))
	scriptName := strings.TrimSpace(readStringConfig(request.GetAdminConfigValues(), "script_name", ""))
	runIntervalMinutes := readAdminScriptRunIntervalMinutes(request.GetAdminConfigValues())
	if shouldSkipDetectionByInterval(request.GetLastSuccessfulRun(), runIntervalMinutes*60) {
		_ = sender.SendActivity(buildDetectorActivity(
			"skipped_by_interval",
			fmt.Sprintf("ADMIN SCRIPT: Detection skipped due to run interval (%dm)", runIntervalMinutes),
			map[string]*plugin_pb.ConfigValue{
				"run_interval_minutes": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(runIntervalMinutes)},
				},
			},
		))
		if err := sender.SendProposals(&plugin_pb.DetectionProposals{
			JobType:   adminScriptJobType,
			Proposals: []*plugin_pb.JobProposal{},
			HasMore:   false,
		}); err != nil {
			return err
		}
		return sender.SendComplete(&plugin_pb.DetectionComplete{
			JobType:        adminScriptJobType,
			Success:        true,
			TotalProposals: 0,
		})
	}

	commands := parseAdminScriptCommands(script)
	execCount := countExecutableCommands(commands)
	if execCount == 0 {
		_ = sender.SendActivity(buildDetectorActivity(
			"no_script",
			"ADMIN SCRIPT: No executable commands configured",
			map[string]*plugin_pb.ConfigValue{
				"command_count": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(execCount)},
				},
			},
		))
		if err := sender.SendProposals(&plugin_pb.DetectionProposals{
			JobType:   adminScriptJobType,
			Proposals: []*plugin_pb.JobProposal{},
			HasMore:   false,
		}); err != nil {
			return err
		}
		return sender.SendComplete(&plugin_pb.DetectionComplete{
			JobType:        adminScriptJobType,
			Success:        true,
			TotalProposals: 0,
		})
	}

	proposal := buildAdminScriptProposal(script, scriptName, execCount)
	proposals := []*plugin_pb.JobProposal{proposal}
	hasMore := false
	maxResults := int(request.MaxResults)
	if maxResults > 0 && len(proposals) > maxResults {
		proposals = proposals[:maxResults]
		hasMore = true
	}

	if err := sender.SendProposals(&plugin_pb.DetectionProposals{
		JobType:   adminScriptJobType,
		Proposals: proposals,
		HasMore:   hasMore,
	}); err != nil {
		return err
	}

	return sender.SendComplete(&plugin_pb.DetectionComplete{
		JobType:        adminScriptJobType,
		Success:        true,
		TotalProposals: 1,
	})
}

func (h *AdminScriptHandler) Execute(ctx context.Context, request *plugin_pb.ExecuteJobRequest, sender ExecutionSender) error {
	if request == nil || request.Job == nil {
		return fmt.Errorf("execute job request is nil")
	}
	if sender == nil {
		return fmt.Errorf("execution sender is nil")
	}
	if request.Job.JobType != "" && request.Job.JobType != adminScriptJobType {
		return fmt.Errorf("job type %q is not handled by admin_script worker", request.Job.JobType)
	}

	script := normalizeAdminScript(readStringConfig(request.Job.Parameters, "script", ""))
	scriptName := strings.TrimSpace(readStringConfig(request.Job.Parameters, "script_name", ""))
	if script == "" {
		script = normalizeAdminScript(readStringConfig(request.GetAdminConfigValues(), "script", ""))
	}
	if scriptName == "" {
		scriptName = strings.TrimSpace(readStringConfig(request.GetAdminConfigValues(), "script_name", ""))
	}

	commands := parseAdminScriptCommands(script)
	execCommands := filterExecutableCommands(commands)
	if len(execCommands) == 0 {
		return sender.SendCompleted(&plugin_pb.JobCompleted{
			Success:      false,
			ErrorMessage: "no executable admin script commands configured",
		})
	}

	commandEnv, cancel, err := h.buildAdminScriptCommandEnv(ctx, request.ClusterContext)
	if err != nil {
		return sender.SendCompleted(&plugin_pb.JobCompleted{
			Success:      false,
			ErrorMessage: err.Error(),
		})
	}
	defer cancel()

	if err := sender.SendProgress(&plugin_pb.JobProgressUpdate{
		JobId:           request.Job.JobId,
		JobType:         request.Job.JobType,
		State:           plugin_pb.JobState_JOB_STATE_ASSIGNED,
		ProgressPercent: 0,
		Stage:           "assigned",
		Message:         "admin script job accepted",
		Activities: []*plugin_pb.ActivityEvent{
			buildExecutorActivity("assigned", "admin script job accepted"),
		},
	}); err != nil {
		return err
	}

	output := &limitedBuffer{maxBytes: maxAdminScriptOutputBytes}
	executed := 0
	errorMessages := make([]string, 0)
	executedCommands := make([]string, 0, len(execCommands))

	for _, cmd := range execCommands {
		if ctx.Err() != nil {
			errorMessages = append(errorMessages, ctx.Err().Error())
			break
		}

		commandLine := formatAdminScriptCommand(cmd)
		executedCommands = append(executedCommands, commandLine)
		_, _ = fmt.Fprintf(output, "$ %s\n", commandLine)

		found := false
		for _, command := range shell.Commands {
			if command.Name() != cmd.Name {
				continue
			}
			found = true
			if err := command.Do(cmd.Args, commandEnv, output); err != nil {
				msg := fmt.Sprintf("%s: %v", cmd.Name, err)
				errorMessages = append(errorMessages, msg)
				_ = sender.SendProgress(&plugin_pb.JobProgressUpdate{
					State:           plugin_pb.JobState_JOB_STATE_RUNNING,
					ProgressPercent: percentProgress(executed+1, len(execCommands)),
					Stage:           "error",
					Message:         msg,
					Activities: []*plugin_pb.ActivityEvent{
						buildExecutorActivity("error", msg),
					},
				})
			}
			break
		}

		if !found {
			msg := fmt.Sprintf("unknown admin command: %s", cmd.Name)
			errorMessages = append(errorMessages, msg)
			_ = sender.SendProgress(&plugin_pb.JobProgressUpdate{
				State:           plugin_pb.JobState_JOB_STATE_RUNNING,
				ProgressPercent: percentProgress(executed+1, len(execCommands)),
				Stage:           "error",
				Message:         msg,
				Activities: []*plugin_pb.ActivityEvent{
					buildExecutorActivity("error", msg),
				},
			})
		}

		executed++
		progress := percentProgress(executed, len(execCommands))
		_ = sender.SendProgress(&plugin_pb.JobProgressUpdate{
			State:           plugin_pb.JobState_JOB_STATE_RUNNING,
			ProgressPercent: progress,
			Stage:           "running",
			Message:         fmt.Sprintf("executed %d/%d command(s)", executed, len(execCommands)),
			Activities: []*plugin_pb.ActivityEvent{
				buildExecutorActivity("running", commandLine),
			},
		})
	}

	scriptHash := hashAdminScript(script)
	resultSummary := fmt.Sprintf("admin script executed (%d command(s))", executed)
	if scriptName != "" {
		resultSummary = fmt.Sprintf("admin script %q executed (%d command(s))", scriptName, executed)
	}

	outputValues := map[string]*plugin_pb.ConfigValue{
		"command_count": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(executed)},
		},
		"error_count": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(len(errorMessages))},
		},
		"script_hash": {
			Kind: &plugin_pb.ConfigValue_StringValue{StringValue: scriptHash},
		},
	}
	if scriptName != "" {
		outputValues["script_name"] = &plugin_pb.ConfigValue{
			Kind: &plugin_pb.ConfigValue_StringValue{StringValue: scriptName},
		}
	}
	if len(executedCommands) > 0 {
		outputValues["commands"] = &plugin_pb.ConfigValue{
			Kind: &plugin_pb.ConfigValue_StringList{
				StringList: &plugin_pb.StringList{Values: executedCommands},
			},
		}
	}
	if out := strings.TrimSpace(output.String()); out != "" {
		outputValues["output"] = &plugin_pb.ConfigValue{
			Kind: &plugin_pb.ConfigValue_StringValue{StringValue: out},
		}
	}
	if output.truncated {
		outputValues["output_truncated"] = &plugin_pb.ConfigValue{
			Kind: &plugin_pb.ConfigValue_BoolValue{BoolValue: true},
		}
	}

	success := len(errorMessages) == 0 && ctx.Err() == nil
	errorMessage := ""
	if !success {
		errorMessage = strings.Join(errorMessages, "; ")
		if ctx.Err() != nil {
			if errorMessage == "" {
				errorMessage = ctx.Err().Error()
			} else {
				errorMessage = fmt.Sprintf("%s; %s", errorMessage, ctx.Err().Error())
			}
		}
	}

	return sender.SendCompleted(&plugin_pb.JobCompleted{
		Success:      success,
		ErrorMessage: errorMessage,
		Result: &plugin_pb.JobResult{
			Summary:      resultSummary,
			OutputValues: outputValues,
		},
		Activities: []*plugin_pb.ActivityEvent{
			buildExecutorActivity("completed", resultSummary),
		},
		CompletedAt: timestamppb.Now(),
	})
}

func readAdminScriptRunIntervalMinutes(values map[string]*plugin_pb.ConfigValue) int {
	runIntervalMinutes := int(readInt64Config(values, "run_interval_minutes", defaultAdminScriptRunMins))
	if runIntervalMinutes <= 0 {
		return defaultAdminScriptRunMins
	}
	return runIntervalMinutes
}

type adminScriptCommand struct {
	Name string
	Args []string
	Raw  string
}

func normalizeAdminScript(script string) string {
	script = strings.ReplaceAll(script, "\r\n", "\n")
	return strings.TrimSpace(script)
}

func parseAdminScriptCommands(script string) []adminScriptCommand {
	script = normalizeAdminScript(script)
	if script == "" {
		return nil
	}
	lines := strings.Split(script, "\n")
	commands := make([]adminScriptCommand, 0)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		for _, chunk := range strings.Split(line, ";") {
			chunk = strings.TrimSpace(chunk)
			if chunk == "" {
				continue
			}
			parts := adminScriptTokenRegex.FindAllString(chunk, -1)
			if len(parts) == 0 {
				continue
			}
			args := make([]string, 0, len(parts)-1)
			for _, arg := range parts[1:] {
				args = append(args, strings.Trim(arg, "\"'"))
			}
			commands = append(commands, adminScriptCommand{
				Name: strings.TrimSpace(parts[0]),
				Args: args,
				Raw:  chunk,
			})
		}
	}
	return commands
}

func filterExecutableCommands(commands []adminScriptCommand) []adminScriptCommand {
	exec := make([]adminScriptCommand, 0, len(commands))
	for _, cmd := range commands {
		if cmd.Name == "" {
			continue
		}
		if isAdminScriptLockCommand(cmd.Name) {
			continue
		}
		exec = append(exec, cmd)
	}
	return exec
}

func countExecutableCommands(commands []adminScriptCommand) int {
	count := 0
	for _, cmd := range commands {
		if cmd.Name == "" {
			continue
		}
		if isAdminScriptLockCommand(cmd.Name) {
			continue
		}
		count++
	}
	return count
}

func isAdminScriptLockCommand(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "lock", "unlock":
		return true
	default:
		return false
	}
}

func buildAdminScriptProposal(script, scriptName string, commandCount int) *plugin_pb.JobProposal {
	scriptHash := hashAdminScript(script)
	summary := "Run admin script"
	if scriptName != "" {
		summary = fmt.Sprintf("Run admin script: %s", scriptName)
	}
	detail := fmt.Sprintf("Admin script with %d command(s)", commandCount)
	proposalID := fmt.Sprintf("admin-script-%s-%d", scriptHash[:8], time.Now().UnixNano())

	labels := map[string]string{
		"script_hash": scriptHash,
	}
	if scriptName != "" {
		labels["script_name"] = scriptName
	}

	return &plugin_pb.JobProposal{
		ProposalId: proposalID,
		DedupeKey:  "admin-script:" + scriptHash,
		JobType:    adminScriptJobType,
		Priority:   plugin_pb.JobPriority_JOB_PRIORITY_NORMAL,
		Summary:    summary,
		Detail:     detail,
		Parameters: map[string]*plugin_pb.ConfigValue{
			"script": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: script},
			},
			"script_name": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: scriptName},
			},
			"script_hash": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: scriptHash},
			},
			"command_count": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(commandCount)},
			},
		},
		Labels: labels,
	}
}

func (h *AdminScriptHandler) buildAdminScriptCommandEnv(
	ctx context.Context,
	clusterContext *plugin_pb.ClusterContext,
) (*shell.CommandEnv, context.CancelFunc, error) {
	if clusterContext == nil {
		return nil, nil, fmt.Errorf("cluster context is required")
	}

	masters := normalizeAddressList(clusterContext.MasterGrpcAddresses)
	if len(masters) == 0 {
		return nil, nil, fmt.Errorf("missing master addresses for admin script")
	}

	filerGroup := ""
	mastersValue := strings.Join(masters, ",")
	options := shell.ShellOptions{
		Masters:        &mastersValue,
		GrpcDialOption: h.grpcDialOption,
		FilerGroup:     &filerGroup,
		Directory:      "/",
	}

	filers := normalizeAddressList(clusterContext.FilerGrpcAddresses)
	if len(filers) > 0 {
		options.FilerAddress = pb.ServerAddress(filers[0])
	} else {
		glog.V(1).Infof("admin script worker missing filer address; filer-dependent commands may fail")
	}

	commandEnv := shell.NewCommandEnv(&options)
	commandEnv.SetNoLock(true)

	ctx, cancel := context.WithCancel(ctx)
	go commandEnv.MasterClient.KeepConnectedToMaster(ctx)

	return commandEnv, cancel, nil
}

func normalizeAddressList(addresses []string) []string {
	normalized := make([]string, 0, len(addresses))
	seen := make(map[string]struct{}, len(addresses))
	for _, address := range addresses {
		address = strings.TrimSpace(address)
		if address == "" {
			continue
		}
		if _, exists := seen[address]; exists {
			continue
		}
		seen[address] = struct{}{}
		normalized = append(normalized, address)
	}
	return normalized
}

func hashAdminScript(script string) string {
	sum := sha256.Sum256([]byte(script))
	return hex.EncodeToString(sum[:])
}

func formatAdminScriptCommand(cmd adminScriptCommand) string {
	if len(cmd.Args) == 0 {
		return cmd.Name
	}
	return fmt.Sprintf("%s %s", cmd.Name, strings.Join(cmd.Args, " "))
}

func percentProgress(done, total int) float64 {
	if total <= 0 {
		return 0
	}
	if done < 0 {
		done = 0
	}
	if done > total {
		done = total
	}
	return float64(done) / float64(total) * 100
}

type limitedBuffer struct {
	buf       bytes.Buffer
	maxBytes  int
	truncated bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if b == nil {
		return len(p), nil
	}
	if b.maxBytes <= 0 {
		b.truncated = true
		return len(p), nil
	}
	remaining := b.maxBytes - b.buf.Len()
	if remaining <= 0 {
		b.truncated = true
		return len(p), nil
	}
	if len(p) > remaining {
		_, _ = b.buf.Write(p[:remaining])
		b.truncated = true
		return len(p), nil
	}
	_, _ = b.buf.Write(p)
	return len(p), nil
}

func (b *limitedBuffer) String() string {
	if b == nil {
		return ""
	}
	return b.buf.String()
}
