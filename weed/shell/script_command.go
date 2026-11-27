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
