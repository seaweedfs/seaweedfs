package shell

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

var LoadConfig = loadConfig

func init() {
	Commands = append(Commands, &commandS3CircuitBreaker{})
}

type commandS3CircuitBreaker struct {
}

func (c *commandS3CircuitBreaker) Name() string {
	return "s3.circuitBreaker"
}

func (c *commandS3CircuitBreaker) Help() string {
	return `configure and apply s3 circuit breaker options for each bucket

	# examples
	# add circuit breaker config for global
	s3.circuitBreaker -global -type count -actions Read,Write -values 500,200 -apply

	# disable global config
	s3.circuitBreaker -global -disable -apply

	# add circuit breaker config for buckets x,y,z
	s3.circuitBreaker -buckets x,y,z -type count -actions Read,Write -values 200,100 -apply

	# disable circuit breaker config of x
	s3.circuitBreaker -buckets x -disable -apply

	# delete circuit breaker config of x
	s3.circuitBreaker -buckets x -delete -apply

	# clear all circuit breaker config
	s3.circuitBreaker -delete -apply
	`
}

func (c *commandS3CircuitBreaker) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3CircuitBreaker) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	dir := s3_constants.CircuitBreakerConfigDir
	file := s3_constants.CircuitBreakerConfigFile

	s3CircuitBreakerCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	buckets := s3CircuitBreakerCommand.String("buckets", "", "the bucket name(s) to configure, eg: -buckets x,y,z")
	global := s3CircuitBreakerCommand.Bool("global", false, "configure global circuit breaker")

	actions := s3CircuitBreakerCommand.String("actions", "", "comma separated actions names: Read,Write,List,Tagging,Admin")
	limitType := s3CircuitBreakerCommand.String("type", "", "'Count' or 'MB'; Count represents the number of simultaneous requests, and MB represents the content size of all simultaneous requests")
	values := s3CircuitBreakerCommand.String("values", "", "comma separated values")

	disabled := s3CircuitBreakerCommand.Bool("disable", false, "disable global or buckets circuit breaker")
	deleted := s3CircuitBreakerCommand.Bool("delete", false, "delete circuit breaker config")

	apply := s3CircuitBreakerCommand.Bool("apply", false, "update and apply current configuration")

	if err = s3CircuitBreakerCommand.Parse(args); err != nil {
		return nil

	}

	var buf bytes.Buffer
	err = LoadConfig(commandEnv, dir, file, &buf)
	if err != nil {
		return err
	}

	cbCfg := &s3_pb.S3CircuitBreakerConfig{
		Buckets: make(map[string]*s3_pb.S3CircuitBreakerOptions),
	}
	if buf.Len() > 0 {
		if err = filer.ParseS3ConfigurationFromBytes(buf.Bytes(), cbCfg); err != nil {
			return err
		}
	}

	if *deleted {
		cmdBuckets, cmdActions, _, err := c.initActionsAndValues(buckets, actions, limitType, values, true)
		if err != nil {
			return err
		}

		if len(cmdBuckets) <= 0 && !*global {
			if len(cmdActions) > 0 {
				deleteGlobalActions(cbCfg, cmdActions, limitType)
				if cbCfg.Buckets != nil {
					var allBuckets []string
					for bucket := range cbCfg.Buckets {
						allBuckets = append(allBuckets, bucket)
					}
					deleteBucketsActions(allBuckets, cbCfg, cmdActions, limitType)
				}
			} else {
				cbCfg.Global = nil
				cbCfg.Buckets = nil
			}
		} else {
			if len(cmdBuckets) > 0 {
				deleteBucketsActions(cmdBuckets, cbCfg, cmdActions, limitType)
			}
			if *global {
				deleteGlobalActions(cbCfg, cmdActions, nil)
			}
		}
	} else {
		cmdBuckets, cmdActions, cmdValues, err := c.initActionsAndValues(buckets, actions, limitType, values, *disabled)
		if err != nil {
			return err
		}

		if len(cmdActions) > 0 && len(*buckets) <= 0 && !*global {
			return fmt.Errorf("one of -global and -buckets must be specified")
		}

		if len(*buckets) > 0 {
			for _, bucket := range cmdBuckets {
				var cbOptions *s3_pb.S3CircuitBreakerOptions
				var exists bool
				if cbOptions, exists = cbCfg.Buckets[bucket]; !exists {
					cbOptions = &s3_pb.S3CircuitBreakerOptions{}
					cbCfg.Buckets[bucket] = cbOptions
				}
				cbOptions.Enabled = !*disabled

				if len(cmdActions) > 0 {
					err = insertOrUpdateValues(cbOptions, cmdActions, cmdValues, limitType)
					if err != nil {
						return err
					}
				}

				if len(cbOptions.Actions) <= 0 && !cbOptions.Enabled {
					delete(cbCfg.Buckets, bucket)
				}
			}
		}

		if *global {
			globalOptions := cbCfg.Global
			if globalOptions == nil {
				globalOptions = &s3_pb.S3CircuitBreakerOptions{Actions: make(map[string]int64, len(cmdActions))}
				cbCfg.Global = globalOptions
			}
			globalOptions.Enabled = !*disabled

			if len(cmdActions) > 0 {
				err = insertOrUpdateValues(globalOptions, cmdActions, cmdValues, limitType)
				if err != nil {
					return err
				}
			}

			if len(globalOptions.Actions) <= 0 && !globalOptions.Enabled {
				cbCfg.Global = nil
			}
		}
	}

	buf.Reset()
	err = filer.ProtoToText(&buf, cbCfg)
	if err != nil {
		return err
	}

	fmt.Fprint(writer, buf.String())
	fmt.Fprintln(writer)

	if *apply {
		if err := commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			return filer.SaveInsideFiler(client, dir, file, buf.Bytes())
		}); err != nil {
			return err
		}
	}

	return nil
}

func loadConfig(commandEnv *CommandEnv, dir string, file string, buf *bytes.Buffer) error {
	if err := commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer.ReadEntry(commandEnv.MasterClient, client, dir, file, buf)
	}); err != nil && err != filer_pb.ErrNotFound {
		return err
	}
	return nil
}

func insertOrUpdateValues(cbOptions *s3_pb.S3CircuitBreakerOptions, cmdActions []string, cmdValues []int64, limitType *string) error {
	if len(*limitType) == 0 {
		return fmt.Errorf("type not valid, only 'count' and 'bytes' are allowed")
	}

	if cbOptions.Actions == nil {
		cbOptions.Actions = make(map[string]int64, len(cmdActions))
	}

	if len(cmdValues) > 0 {
		for i, action := range cmdActions {
			cbOptions.Actions[s3_constants.Concat(action, *limitType)] = cmdValues[i]
		}
	}
	return nil
}

func deleteBucketsActions(cmdBuckets []string, cbCfg *s3_pb.S3CircuitBreakerConfig, cmdActions []string, limitType *string) {
	if cbCfg.Buckets == nil {
		return
	}

	if len(cmdActions) == 0 {
		for _, bucket := range cmdBuckets {
			delete(cbCfg.Buckets, bucket)
		}
	} else {
		for _, bucket := range cmdBuckets {
			if cbOption, ok := cbCfg.Buckets[bucket]; ok {
				if len(cmdActions) > 0 && cbOption.Actions != nil {
					for _, action := range cmdActions {
						delete(cbOption.Actions, s3_constants.Concat(action, *limitType))
					}
				}

				if len(cbOption.Actions) == 0 && !cbOption.Enabled {
					delete(cbCfg.Buckets, bucket)
				}
			}
		}
	}

	if len(cbCfg.Buckets) == 0 {
		cbCfg.Buckets = nil
	}
}

func deleteGlobalActions(cbCfg *s3_pb.S3CircuitBreakerConfig, cmdActions []string, limitType *string) {
	globalOptions := cbCfg.Global
	if globalOptions == nil {
		return
	}

	if len(cmdActions) == 0 && globalOptions.Actions != nil {
		globalOptions.Actions = nil
		return
	} else {
		for _, action := range cmdActions {
			delete(globalOptions.Actions, s3_constants.Concat(action, *limitType))
		}
	}

	if len(globalOptions.Actions) == 0 && !globalOptions.Enabled {
		cbCfg.Global = nil
	}
}

func (c *commandS3CircuitBreaker) initActionsAndValues(buckets, actions, limitType, values *string, parseValues bool) (cmdBuckets, cmdActions []string, cmdValues []int64, err error) {
	if len(*buckets) > 0 {
		cmdBuckets = strings.Split(*buckets, ",")
	}

	if len(*actions) > 0 {
		cmdActions = strings.Split(*actions, ",")

		//check action valid
		for _, action := range cmdActions {
			var found bool
			for _, allowedAction := range s3_constants.AllowedActions {
				if allowedAction == action {
					found = true
				}
			}
			if !found {
				return nil, nil, nil, fmt.Errorf("value(%s) of flag[-action] not valid, allowed actions: %v", *actions, s3_constants.AllowedActions)
			}
		}
	}

	if !parseValues {
		if len(cmdActions) < 0 {
			for _, action := range s3_constants.AllowedActions {
				cmdActions = append(cmdActions, action)
			}
		}

		if len(*limitType) > 0 {
			switch *limitType {
			case s3_constants.LimitTypeCount:
				elements := strings.Split(*values, ",")
				if len(cmdActions) != len(elements) {
					if len(elements) != 1 || len(elements) == 0 {
						return nil, nil, nil, fmt.Errorf("count of flag[-actions] and flag[-counts] not equal")
					}
					v, err := strconv.Atoi(elements[0])
					if err != nil {
						return nil, nil, nil, fmt.Errorf("value of -values must be a legal number(s)")
					}
					for range cmdActions {
						cmdValues = append(cmdValues, int64(v))
					}
				} else {
					for _, value := range elements {
						v, err := strconv.Atoi(value)
						if err != nil {
							return nil, nil, nil, fmt.Errorf("value of -values must be a legal number(s)")
						}
						cmdValues = append(cmdValues, int64(v))
					}
				}
			case s3_constants.LimitTypeBytes:
				elements := strings.Split(*values, ",")
				if len(cmdActions) != len(elements) {
					if len(elements) != 1 || len(elements) == 0 {
						return nil, nil, nil, fmt.Errorf("values count of -actions and -values not equal")
					}
					v, err := parseMBToBytes(elements[0])
					if err != nil {
						return nil, nil, nil, fmt.Errorf("value of -max must be a legal number(s)")
					}
					for range cmdActions {
						cmdValues = append(cmdValues, v)
					}
				} else {
					for _, value := range elements {
						v, err := parseMBToBytes(value)
						if err != nil {
							return nil, nil, nil, fmt.Errorf("value of -max must be a legal number(s)")
						}
						cmdValues = append(cmdValues, v)
					}
				}
			default:
				return nil, nil, nil, fmt.Errorf("type not valid, only 'count' and 'bytes' are allowed")
			}
		} else {
			*limitType = ""
		}
	}
	return cmdBuckets, cmdActions, cmdValues, nil
}

func parseMBToBytes(valStr string) (int64, error) {
	v, err := strconv.Atoi(valStr)
	v *= 1024 * 1024
	return int64(v), err
}
