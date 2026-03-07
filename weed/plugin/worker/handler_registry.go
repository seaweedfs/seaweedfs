package pluginworker

import (
	"fmt"
	"strings"
	"sync"

	"google.golang.org/grpc"
)

// JobCategory groups job types by resource profile so that workers can be
// configured with a category name instead of an explicit list of job types.
type JobCategory string

const (
	CategoryDefault JobCategory = "default" // lightweight, safe for any worker
	CategoryHeavy   JobCategory = "heavy"   // resource-intensive jobs
)

// HandlerFactory describes how to build a JobHandler for a single job type.
type HandlerFactory struct {
	// JobType is the canonical job type string (e.g. "vacuum").
	JobType string
	// Category controls which category label selects this handler.
	Category JobCategory
	// Aliases are alternative CLI names that resolve to this job type
	// (e.g. "ec" for "erasure_coding").
	Aliases []string
	// Build constructs the JobHandler.
	Build func(opts HandlerBuildOptions) (JobHandler, error)
}

// HandlerBuildOptions carries parameters forwarded from the CLI to handler
// constructors.
type HandlerBuildOptions struct {
	GrpcDialOption grpc.DialOption
	MaxExecute     int
	WorkingDir     string
}

var (
	registryMu sync.Mutex
	registry   []HandlerFactory
)

// RegisterHandler adds a handler factory to the global registry.
// It is intended to be called from handler init() functions.
func RegisterHandler(f HandlerFactory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry = append(registry, f)
}

// ResolveHandlerFactories takes a comma-separated token list that can contain
// category names ("all", "default", "heavy") and/or explicit job type names
// (including aliases). It returns a deduplicated, ordered slice of factories.
func ResolveHandlerFactories(tokens string) ([]HandlerFactory, error) {
	registryMu.Lock()
	snapshot := make([]HandlerFactory, len(registry))
	copy(snapshot, registry)
	registryMu.Unlock()

	parts := strings.Split(tokens, ",")
	var result []HandlerFactory
	seen := make(map[string]bool)

	for _, raw := range parts {
		tok := strings.ToLower(strings.TrimSpace(raw))
		if tok == "" {
			continue
		}

		if cat, ok := tokenAsCategory(tok); ok {
			for _, f := range snapshot {
				if cat == "all" || f.Category == cat {
					if !seen[f.JobType] {
						seen[f.JobType] = true
						result = append(result, f)
					}
				}
			}
			continue
		}

		f, err := findFactory(snapshot, tok)
		if err != nil {
			return nil, err
		}
		if !seen[f.JobType] {
			seen[f.JobType] = true
			result = append(result, f)
		}
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("no job types resolved from %q", tokens)
	}
	return result, nil
}

// tokenAsCategory returns the category and true when tok is a known category
// keyword. "all" is treated as a special pseudo-category that matches every
// registered handler.
func tokenAsCategory(tok string) (JobCategory, bool) {
	switch tok {
	case "all":
		return "all", true
	case "default":
		return CategoryDefault, true
	case "heavy":
		return CategoryHeavy, true
	default:
		return "", false
	}
}

func findFactory(factories []HandlerFactory, tok string) (HandlerFactory, error) {
	for _, f := range factories {
		if strings.EqualFold(f.JobType, tok) {
			return f, nil
		}
		for _, alias := range f.Aliases {
			if strings.EqualFold(alias, tok) {
				return f, nil
			}
		}
	}
	return HandlerFactory{}, fmt.Errorf("unknown job type %q", tok)
}
