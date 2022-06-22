package shell

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/pb/s3_pb"
	"github.com/chrislusf/seaweedfs/weed/s3api/s3_constants"
	"io"
	"strconv"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
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
	s3.circuitBreaker -global -type Count -actions Read,Write -values 500,200 -apply

	# disable global config
	s3.circuitBreaker -global -disable -apply

	# add circuit breaker config for buckets x,y,z
	s3.circuitBreaker -buckets x,y,z -type Count -actions Read,Write -values 200,100 -apply

	# disable circuit breaker config of x
	s3.circuitBreaker -buckets x -disable -apply

	# delete circuit breaker config of x
	s3.circuitBreaker -buckets x -delete -apply

	# clear all circuit breaker config
	s3.circuitBreaker -delete -apply
	`
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
		cmdBuckets, cmdActions, _, err := c.initActionsAndValues(buckets, actions, limitType, values, false)
		if err != nil {
			return err
		}

		if len(cmdBuckets) == 0 && !*global {
			deleteGlobalActions(cbCfg, cmdActions, limitType)
			for bucket := range cbCfg.Buckets {
				cmdBuckets = append(cmdBuckets, bucket)
			}
			deleteBucketsActions(cmdBuckets, cbCfg, cmdActions, limitType)
		} else {
			if len(cmdBuckets) > 0 {
				deleteBucketsActions(cmdBuckets, cbCfg, cmdActions, limitType)
			}
			if *global {
				deleteGlobalActions(cbCfg, cmdActions, limitType)
			}
		}
	} else {
		cmdBuckets, cmdActions, cmdValues, err := c.initActionsAndValues(buckets, actions, limitType, values, !*disabled)
		if err != nil {
			return err
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
				globalOptions = &s3_pb.S3CircuitBreakerOptions{Actions: make(map[string]string, len(cmdActions))}
				cbCfg.Global = globalOptions
			}
			globalOptions.Enabled = !*disabled

			if *limitType != "" {
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

	_, _ = fmt.Fprintf(writer, string(buf.Bytes()))
	_, _ = fmt.Fprintln(writer)

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

func insertOrUpdateValues(cbOptions *s3_pb.S3CircuitBreakerOptions, cmdActions []string, cmdValues []string, limitType *string) error {
	if cbOptions.Actions == nil {
		cbOptions.Actions = make(map[string]string, len(cmdActions))
	}

	if len(cmdValues) > 0 {
		for i, action := range cmdActions {
			cbOptions.Actions[s3_constants.Concat(action, *limitType)] = cmdValues[i]
		}
	}
	return nil
}

func deleteBucketsActions(cmdBuckets []string, cbCfg *s3_pb.S3CircuitBreakerConfig, cmdActions []string, limitType *string) {
	if len(cbCfg.Buckets) == 0 {
		return
	}

	deletedActions := cmdActions
	if len(cmdActions) == 0 {
		deletedActions = s3_constants.AllowedActions
	}

	for _, bucket := range cmdBuckets {
		if bucketOptions, ok := cbCfg.Buckets[bucket]; ok {
			if len(cmdActions) == 0 && *limitType == "" {
				delete(cbCfg.Buckets, bucket)
				continue
			}

			if len(deletedActions) > 0 && len(bucketOptions.Actions) > 0 {
				for _, action := range deletedActions {
					deleteAction(bucketOptions.Actions, action, limitType)
				}
			}

			if len(bucketOptions.Actions) == 0 && !bucketOptions.Enabled {
				delete(cbCfg.Buckets, bucket)
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

	if len(cmdActions) == 0 && *limitType == "" {
		cbCfg.Global = nil
		return
	}

	deletedActions := cmdActions
	if len(cmdActions) == 0 {
		deletedActions = s3_constants.AllowedActions
	}

	if len(globalOptions.Actions) > 0 {
		for _, action := range deletedActions {
			deleteAction(globalOptions.Actions, action, limitType)
		}
	}

	if len(globalOptions.Actions) == 0 && !globalOptions.Enabled {
		cbCfg.Global = nil
	}
}

func deleteAction(actions map[string]string, action string, limitType *string) {
	if *limitType != "" {
		delete(actions, s3_constants.Concat(action, *limitType))
	} else {
		delete(actions, s3_constants.Concat(action, s3_constants.LimitTypeCount))
		delete(actions, s3_constants.Concat(action, s3_constants.LimitTypeMB))
	}
}

func (c *commandS3CircuitBreaker) initActionsAndValues(buckets, actions, limitType, values *string, parseValues bool) (cmdBuckets, cmdActions []string, cmdValues []string, err error) {
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

	if *limitType != "" && !(*limitType == s3_constants.LimitTypeMB || *limitType == s3_constants.LimitTypeCount) {
		return nil, nil, nil, fmt.Errorf("type not valid, only 'count' and 'MB' are allowed")
	}

	if parseValues {
		if len(cmdActions) == 0 {
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
						return nil, nil, nil, fmt.Errorf("values count of -actions and --values not equal")
					}
					//validate values
					_, err := strconv.Atoi(elements[0])
					if err != nil {
						return nil, nil, nil, fmt.Errorf("value of -values must be a legal int number(s)")
					}
					for range cmdActions {
						cmdValues = append(cmdValues, elements[0])
					}
				} else {
					for _, value := range elements {
						_, err := strconv.Atoi(value)
						if err != nil {
							return nil, nil, nil, fmt.Errorf("value of -values must be a legal int number(s)")
						}
						cmdValues = append(cmdValues, value)
					}
				}
			case s3_constants.LimitTypeMB:
				elements := strings.Split(*values, ",")
				if len(cmdActions) != len(elements) {
					if len(elements) != 1 || len(elements) == 0 {
						return nil, nil, nil, fmt.Errorf("values count of -actions and -values not equal")
					}
					//validate values
					_, err := strconv.ParseFloat(elements[0], 64)
					if err != nil {
						return nil, nil, nil, fmt.Errorf("value must be a legal number(s)")
					}
					for range cmdActions {
						cmdValues = append(cmdValues, elements[0])
					}
				} else {
					for _, value := range elements {
						_, err := strconv.ParseFloat(value, 64)
						if err != nil {
							return nil, nil, nil, fmt.Errorf("value must be a legal number(s)")
						}
						cmdValues = append(cmdValues, value)
					}
				}
			}
		}
	}
	return cmdBuckets, cmdActions, cmdValues, nil
}
