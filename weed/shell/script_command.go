package shell

import (
	"regexp"
	"strings"
)

var scriptTokenRegex = regexp.MustCompile(`'.*?'|".*?"|\S+`)

// SplitScriptCommand splits one admin script command into its name and unquoted arguments.
func SplitScriptCommand(chunk string) (name string, args []string, ok bool) {
	parts := scriptTokenRegex.FindAllString(chunk, -1)
	if len(parts) == 0 {
		return "", nil, false
	}
	args = make([]string, 0, len(parts)-1)
	for _, arg := range parts[1:] {
		args = append(args, strings.Trim(arg, "\"'"))
	}
	return strings.TrimSpace(parts[0]), args, true
}

type argsApplier interface {
	argsApply(args []string) bool
}

// CommandArgsApply reports whether the named command accepts args and changes the cluster with them.
func CommandArgsApply(name string, args []string) bool {
	for _, c := range Commands {
		if c.Name() == name {
			applier, ok := c.(argsApplier)
			return ok && applier.argsApply(args)
		}
	}
	return false
}
