package plugin

import "time"

const (
	defaultSchedulerIdleSleep = 17 * time.Minute
)

type SchedulerConfig struct {
	IdleSleepSeconds int32 `json:"idle_sleep_seconds"`
}

func DefaultSchedulerConfig() SchedulerConfig {
	return SchedulerConfig{
		IdleSleepSeconds: int32(defaultSchedulerIdleSleep / time.Second),
	}
}

func normalizeSchedulerConfig(cfg SchedulerConfig) SchedulerConfig {
	if cfg.IdleSleepSeconds <= 0 {
		return DefaultSchedulerConfig()
	}
	return cfg
}

func (c SchedulerConfig) IdleSleepDuration() time.Duration {
	if c.IdleSleepSeconds <= 0 {
		return defaultSchedulerIdleSleep
	}
	return time.Duration(c.IdleSleepSeconds) * time.Second
}
