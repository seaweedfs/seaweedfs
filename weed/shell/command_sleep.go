package shell

import (
	"flag"
	"fmt"
	"io"
	"strconv"
	"time"
)

func init() {
	Commands = append(Commands, &commandSleep{})
}

// =========== Sleep ==============
type commandSleep struct {
}

func (c *commandSleep) Name() string {
	return "sleep"
}

func (c *commandSleep) Help() string {
	return `sleep for a duration (useful to simulate long running jobs)

	sleep -seconds 5
	sleep -duration 2s
	sleep 1500ms
`
}

func (c *commandSleep) HasTag(CommandTag) bool {
	return false
}

func (c *commandSleep) Do(args []string, _ *CommandEnv, _ io.Writer) error {
	sleepCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	seconds := sleepCommand.Int("seconds", 0, "sleep seconds")
	duration := sleepCommand.Duration("duration", 0, "sleep duration (e.g. 2s, 1m)")
	if err := sleepCommand.Parse(args); err != nil {
		return err
	}

	var sleepFor time.Duration
	if *duration > 0 {
		sleepFor = *duration
	} else if *seconds > 0 {
		sleepFor = time.Duration(*seconds) * time.Second
	} else {
		remaining := sleepCommand.Args()
		if len(remaining) > 0 {
			value := remaining[0]
			parsed, parseErr := time.ParseDuration(value)
			if parseErr != nil {
				if asInt, intErr := strconv.Atoi(value); intErr == nil {
					parsed = time.Duration(asInt) * time.Second
				} else {
					return fmt.Errorf("invalid duration %q", value)
				}
			}
			sleepFor = parsed
		}
	}

	if sleepFor <= 0 {
		return fmt.Errorf("sleep duration is required")
	}

	time.Sleep(sleepFor)
	return nil
}
