package plugin

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	pluginDirName           = "plugin"
	jobTypesDirName         = "job_types"
	jobsDirName             = "jobs"
	jobDetailsDirName       = "job_details"
	activitiesDirName       = "activities"
	descriptorPBFileName    = "descriptor.pb"
	descriptorJSONFileName  = "descriptor.json"
	configPBFileName        = "config.pb"
	configJSONFileName      = "config.json"
	runsJSONFileName        = "runs.json"
	trackedJobsJSONFileName = "tracked_jobs.json"
	activitiesJSONFileName  = "activities.json"
	defaultDirPerm          = 0o755
	defaultFilePerm         = 0o644
)

var validJobTypePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// ConfigStore persists plugin configuration and bounded run history.
// If admin data dir is empty, it transparently falls back to in-memory mode.
type ConfigStore struct {
	configured bool
	baseDir    string

	mu sync.RWMutex

	memDescriptors map[string]*plugin_pb.JobTypeDescriptor
	memConfigs     map[string]*plugin_pb.PersistedJobTypeConfig
	memRunHistory  map[string]*JobTypeRunHistory
	memTrackedJobs []TrackedJob
	memActivities  []JobActivity
	memJobDetails  map[string]TrackedJob
}

func NewConfigStore(adminDataDir string) (*ConfigStore, error) {
	store := &ConfigStore{
		configured:     adminDataDir != "",
		memDescriptors: make(map[string]*plugin_pb.JobTypeDescriptor),
		memConfigs:     make(map[string]*plugin_pb.PersistedJobTypeConfig),
		memRunHistory:  make(map[string]*JobTypeRunHistory),
		memJobDetails:  make(map[string]TrackedJob),
	}

	if adminDataDir == "" {
		return store, nil
	}

	store.baseDir = filepath.Join(adminDataDir, pluginDirName)
	if err := os.MkdirAll(filepath.Join(store.baseDir, jobTypesDirName), defaultDirPerm); err != nil {
		return nil, fmt.Errorf("create plugin job_types dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(store.baseDir, jobsDirName), defaultDirPerm); err != nil {
		return nil, fmt.Errorf("create plugin jobs dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(store.baseDir, jobsDirName, jobDetailsDirName), defaultDirPerm); err != nil {
		return nil, fmt.Errorf("create plugin job_details dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(store.baseDir, activitiesDirName), defaultDirPerm); err != nil {
		return nil, fmt.Errorf("create plugin activities dir: %w", err)
	}

	return store, nil
}

func (s *ConfigStore) IsConfigured() bool {
	return s.configured
}

func (s *ConfigStore) BaseDir() string {
	return s.baseDir
}

func (s *ConfigStore) SaveDescriptor(jobType string, descriptor *plugin_pb.JobTypeDescriptor) error {
	if descriptor == nil {
		return fmt.Errorf("descriptor is nil")
	}
	if _, err := sanitizeJobType(jobType); err != nil {
		return err
	}

	clone := proto.Clone(descriptor).(*plugin_pb.JobTypeDescriptor)
	if clone.JobType == "" {
		clone.JobType = jobType
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.configured {
		s.memDescriptors[jobType] = clone
		return nil
	}

	jobTypeDir, err := s.ensureJobTypeDir(jobType)
	if err != nil {
		return err
	}

	pbPath := filepath.Join(jobTypeDir, descriptorPBFileName)
	jsonPath := filepath.Join(jobTypeDir, descriptorJSONFileName)

	if err := writeProtoFiles(clone, pbPath, jsonPath); err != nil {
		return fmt.Errorf("save descriptor for %s: %w", jobType, err)
	}

	return nil
}

func (s *ConfigStore) LoadDescriptor(jobType string) (*plugin_pb.JobTypeDescriptor, error) {
	if _, err := sanitizeJobType(jobType); err != nil {
		return nil, err
	}

	s.mu.RLock()
	if !s.configured {
		d := s.memDescriptors[jobType]
		s.mu.RUnlock()
		if d == nil {
			return nil, nil
		}
		return proto.Clone(d).(*plugin_pb.JobTypeDescriptor), nil
	}
	s.mu.RUnlock()

	pbPath := filepath.Join(s.baseDir, jobTypesDirName, jobType, descriptorPBFileName)
	data, err := os.ReadFile(pbPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read descriptor for %s: %w", jobType, err)
	}

	var descriptor plugin_pb.JobTypeDescriptor
	if err := proto.Unmarshal(data, &descriptor); err != nil {
		return nil, fmt.Errorf("unmarshal descriptor for %s: %w", jobType, err)
	}
	return &descriptor, nil
}

func (s *ConfigStore) SaveJobTypeConfig(config *plugin_pb.PersistedJobTypeConfig) error {
	if config == nil {
		return fmt.Errorf("job type config is nil")
	}
	if config.JobType == "" {
		return fmt.Errorf("job type config has empty job_type")
	}
	if _, err := sanitizeJobType(config.JobType); err != nil {
		return err
	}

	clone := proto.Clone(config).(*plugin_pb.PersistedJobTypeConfig)

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.configured {
		s.memConfigs[config.JobType] = clone
		return nil
	}

	jobTypeDir, err := s.ensureJobTypeDir(config.JobType)
	if err != nil {
		return err
	}

	pbPath := filepath.Join(jobTypeDir, configPBFileName)
	jsonPath := filepath.Join(jobTypeDir, configJSONFileName)

	if err := writeProtoFiles(clone, pbPath, jsonPath); err != nil {
		return fmt.Errorf("save job type config for %s: %w", config.JobType, err)
	}

	return nil
}

func (s *ConfigStore) LoadJobTypeConfig(jobType string) (*plugin_pb.PersistedJobTypeConfig, error) {
	if _, err := sanitizeJobType(jobType); err != nil {
		return nil, err
	}

	s.mu.RLock()
	if !s.configured {
		cfg := s.memConfigs[jobType]
		s.mu.RUnlock()
		if cfg == nil {
			return nil, nil
		}
		return proto.Clone(cfg).(*plugin_pb.PersistedJobTypeConfig), nil
	}
	s.mu.RUnlock()

	pbPath := filepath.Join(s.baseDir, jobTypesDirName, jobType, configPBFileName)
	data, err := os.ReadFile(pbPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read job type config for %s: %w", jobType, err)
	}

	var config plugin_pb.PersistedJobTypeConfig
	if err := proto.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("unmarshal job type config for %s: %w", jobType, err)
	}

	return &config, nil
}

func (s *ConfigStore) AppendRunRecord(jobType string, record *JobRunRecord) error {
	if record == nil {
		return fmt.Errorf("run record is nil")
	}
	if _, err := sanitizeJobType(jobType); err != nil {
		return err
	}

	if record.JobType == "" {
		record.JobType = jobType
	}
	if record.CompletedAt.IsZero() {
		record.CompletedAt = time.Now().UTC()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	history, err := s.loadRunHistoryLocked(jobType)
	if err != nil {
		return err
	}

	safeRecord := *record
	safeRecord.JobType = jobType

	if safeRecord.Outcome == RunOutcomeSuccess {
		history.SuccessfulRuns = append(history.SuccessfulRuns, safeRecord)
	} else {
		safeRecord.Outcome = RunOutcomeError
		history.ErrorRuns = append(history.ErrorRuns, safeRecord)
	}

	history.SuccessfulRuns = trimRuns(history.SuccessfulRuns, MaxSuccessfulRunHistory)
	history.ErrorRuns = trimRuns(history.ErrorRuns, MaxErrorRunHistory)
	history.LastUpdatedTime = time.Now().UTC()

	return s.saveRunHistoryLocked(jobType, history)
}

func (s *ConfigStore) LoadRunHistory(jobType string) (*JobTypeRunHistory, error) {
	if _, err := sanitizeJobType(jobType); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	history, err := s.loadRunHistoryLocked(jobType)
	if err != nil {
		return nil, err
	}
	return cloneRunHistory(history), nil
}

func (s *ConfigStore) SaveTrackedJobs(jobs []TrackedJob) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	clone := cloneTrackedJobs(jobs)

	if !s.configured {
		s.memTrackedJobs = clone
		return nil
	}

	encoded, err := json.MarshalIndent(clone, "", "  ")
	if err != nil {
		return fmt.Errorf("encode tracked jobs: %w", err)
	}

	path := filepath.Join(s.baseDir, jobsDirName, trackedJobsJSONFileName)
	if err := os.WriteFile(path, encoded, defaultFilePerm); err != nil {
		return fmt.Errorf("write tracked jobs: %w", err)
	}
	return nil
}

func (s *ConfigStore) LoadTrackedJobs() ([]TrackedJob, error) {
	s.mu.RLock()
	if !s.configured {
		out := cloneTrackedJobs(s.memTrackedJobs)
		s.mu.RUnlock()
		return out, nil
	}
	s.mu.RUnlock()

	path := filepath.Join(s.baseDir, jobsDirName, trackedJobsJSONFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read tracked jobs: %w", err)
	}

	var jobs []TrackedJob
	if err := json.Unmarshal(data, &jobs); err != nil {
		return nil, fmt.Errorf("parse tracked jobs: %w", err)
	}
	return cloneTrackedJobs(jobs), nil
}

func (s *ConfigStore) SaveJobDetail(job TrackedJob) error {
	jobID, err := sanitizeJobID(job.JobID)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	clone := cloneTrackedJob(job)
	clone.JobID = jobID

	if !s.configured {
		s.memJobDetails[jobID] = clone
		return nil
	}

	encoded, err := json.MarshalIndent(clone, "", "  ")
	if err != nil {
		return fmt.Errorf("encode job detail: %w", err)
	}

	path := filepath.Join(s.baseDir, jobsDirName, jobDetailsDirName, jobDetailFileName(jobID))
	if err := os.WriteFile(path, encoded, defaultFilePerm); err != nil {
		return fmt.Errorf("write job detail: %w", err)
	}

	return nil
}

func (s *ConfigStore) LoadJobDetail(jobID string) (*TrackedJob, error) {
	jobID, err := sanitizeJobID(jobID)
	if err != nil {
		return nil, err
	}

	s.mu.RLock()
	if !s.configured {
		job, ok := s.memJobDetails[jobID]
		s.mu.RUnlock()
		if !ok {
			return nil, nil
		}
		clone := cloneTrackedJob(job)
		return &clone, nil
	}
	s.mu.RUnlock()

	path := filepath.Join(s.baseDir, jobsDirName, jobDetailsDirName, jobDetailFileName(jobID))
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read job detail: %w", err)
	}

	var job TrackedJob
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, fmt.Errorf("parse job detail: %w", err)
	}
	clone := cloneTrackedJob(job)
	return &clone, nil
}

func (s *ConfigStore) SaveActivities(activities []JobActivity) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	clone := cloneActivities(activities)

	if !s.configured {
		s.memActivities = clone
		return nil
	}

	encoded, err := json.MarshalIndent(clone, "", "  ")
	if err != nil {
		return fmt.Errorf("encode activities: %w", err)
	}

	path := filepath.Join(s.baseDir, activitiesDirName, activitiesJSONFileName)
	if err := os.WriteFile(path, encoded, defaultFilePerm); err != nil {
		return fmt.Errorf("write activities: %w", err)
	}
	return nil
}

func (s *ConfigStore) LoadActivities() ([]JobActivity, error) {
	s.mu.RLock()
	if !s.configured {
		out := cloneActivities(s.memActivities)
		s.mu.RUnlock()
		return out, nil
	}
	s.mu.RUnlock()

	path := filepath.Join(s.baseDir, activitiesDirName, activitiesJSONFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read activities: %w", err)
	}

	var activities []JobActivity
	if err := json.Unmarshal(data, &activities); err != nil {
		return nil, fmt.Errorf("parse activities: %w", err)
	}
	return cloneActivities(activities), nil
}

func (s *ConfigStore) ListJobTypes() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	jobTypeSet := make(map[string]struct{})

	if !s.configured {
		for jobType := range s.memDescriptors {
			jobTypeSet[jobType] = struct{}{}
		}
		for jobType := range s.memConfigs {
			jobTypeSet[jobType] = struct{}{}
		}
		for jobType := range s.memRunHistory {
			jobTypeSet[jobType] = struct{}{}
		}
	} else {
		jobTypesPath := filepath.Join(s.baseDir, jobTypesDirName)
		entries, err := os.ReadDir(jobTypesPath)
		if err != nil {
			if os.IsNotExist(err) {
				return []string{}, nil
			}
			return nil, fmt.Errorf("list job types: %w", err)
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			jobType := strings.TrimSpace(entry.Name())
			if _, err := sanitizeJobType(jobType); err != nil {
				continue
			}
			jobTypeSet[jobType] = struct{}{}
		}
	}

	jobTypes := make([]string, 0, len(jobTypeSet))
	for jobType := range jobTypeSet {
		jobTypes = append(jobTypes, jobType)
	}
	sort.Strings(jobTypes)
	return jobTypes, nil
}

func (s *ConfigStore) loadRunHistoryLocked(jobType string) (*JobTypeRunHistory, error) {
	if !s.configured {
		history, ok := s.memRunHistory[jobType]
		if !ok {
			history = &JobTypeRunHistory{JobType: jobType}
			s.memRunHistory[jobType] = history
		}
		return cloneRunHistory(history), nil
	}

	runsPath := filepath.Join(s.baseDir, jobTypesDirName, jobType, runsJSONFileName)
	data, err := os.ReadFile(runsPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &JobTypeRunHistory{JobType: jobType}, nil
		}
		return nil, fmt.Errorf("read run history for %s: %w", jobType, err)
	}

	var history JobTypeRunHistory
	if err := json.Unmarshal(data, &history); err != nil {
		return nil, fmt.Errorf("parse run history for %s: %w", jobType, err)
	}
	if history.JobType == "" {
		history.JobType = jobType
	}
	return &history, nil
}

func (s *ConfigStore) saveRunHistoryLocked(jobType string, history *JobTypeRunHistory) error {
	if !s.configured {
		s.memRunHistory[jobType] = cloneRunHistory(history)
		return nil
	}

	jobTypeDir, err := s.ensureJobTypeDir(jobType)
	if err != nil {
		return err
	}

	encoded, err := json.MarshalIndent(history, "", "  ")
	if err != nil {
		return fmt.Errorf("encode run history for %s: %w", jobType, err)
	}

	runsPath := filepath.Join(jobTypeDir, runsJSONFileName)
	if err := os.WriteFile(runsPath, encoded, defaultFilePerm); err != nil {
		return fmt.Errorf("write run history for %s: %w", jobType, err)
	}
	return nil
}

func (s *ConfigStore) ensureJobTypeDir(jobType string) (string, error) {
	if !s.configured {
		return "", nil
	}
	jobTypeDir := filepath.Join(s.baseDir, jobTypesDirName, jobType)
	if err := os.MkdirAll(jobTypeDir, defaultDirPerm); err != nil {
		return "", fmt.Errorf("create job type dir for %s: %w", jobType, err)
	}
	return jobTypeDir, nil
}

func sanitizeJobType(jobType string) (string, error) {
	jobType = strings.TrimSpace(jobType)
	if jobType == "" {
		return "", fmt.Errorf("job type is empty")
	}
	if !validJobTypePattern.MatchString(jobType) {
		return "", fmt.Errorf("invalid job type %q: allowed pattern %s", jobType, validJobTypePattern.String())
	}
	return jobType, nil
}

func sanitizeJobID(jobID string) (string, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return "", fmt.Errorf("job id is empty")
	}
	return jobID, nil
}

func jobDetailFileName(jobID string) string {
	return url.PathEscape(jobID) + ".json"
}

func trimRuns(runs []JobRunRecord, maxKeep int) []JobRunRecord {
	if len(runs) == 0 {
		return runs
	}
	sort.Slice(runs, func(i, j int) bool {
		return runs[i].CompletedAt.After(runs[j].CompletedAt)
	})
	if len(runs) > maxKeep {
		runs = runs[:maxKeep]
	}
	return runs
}

func cloneRunHistory(in *JobTypeRunHistory) *JobTypeRunHistory {
	if in == nil {
		return nil
	}
	out := *in
	if in.SuccessfulRuns != nil {
		out.SuccessfulRuns = append([]JobRunRecord(nil), in.SuccessfulRuns...)
	}
	if in.ErrorRuns != nil {
		out.ErrorRuns = append([]JobRunRecord(nil), in.ErrorRuns...)
	}
	return &out
}

func cloneTrackedJobs(in []TrackedJob) []TrackedJob {
	if len(in) == 0 {
		return nil
	}

	out := make([]TrackedJob, len(in))
	for i := range in {
		out[i] = cloneTrackedJob(in[i])
	}
	return out
}

func cloneTrackedJob(in TrackedJob) TrackedJob {
	out := in
	if in.Parameters != nil {
		out.Parameters = make(map[string]interface{}, len(in.Parameters))
		for key, value := range in.Parameters {
			out.Parameters[key] = value
		}
	}
	if in.Labels != nil {
		out.Labels = make(map[string]string, len(in.Labels))
		for key, value := range in.Labels {
			out.Labels[key] = value
		}
	}
	if in.ResultOutputValues != nil {
		out.ResultOutputValues = make(map[string]interface{}, len(in.ResultOutputValues))
		for key, value := range in.ResultOutputValues {
			out.ResultOutputValues[key] = value
		}
	}
	return out
}

func cloneActivities(in []JobActivity) []JobActivity {
	if len(in) == 0 {
		return nil
	}

	out := make([]JobActivity, len(in))
	for i := range in {
		out[i] = in[i]
		if in[i].Details != nil {
			out[i].Details = make(map[string]interface{}, len(in[i].Details))
			for key, value := range in[i].Details {
				out[i].Details[key] = value
			}
		}
	}
	return out
}

func writeProtoFiles(message proto.Message, pbPath string, jsonPath string) error {
	pbData, err := proto.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal protobuf: %w", err)
	}
	if err := os.WriteFile(pbPath, pbData, defaultFilePerm); err != nil {
		return fmt.Errorf("write protobuf file: %w", err)
	}

	jsonData, err := protojson.MarshalOptions{
		Multiline:       true,
		Indent:          "  ",
		EmitUnpopulated: true,
	}.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal json: %w", err)
	}
	if err := os.WriteFile(jsonPath, jsonData, defaultFilePerm); err != nil {
		return fmt.Errorf("write json file: %w", err)
	}

	return nil
}
