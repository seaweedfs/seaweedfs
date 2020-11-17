package log

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
)

const (
	LogLevel = "LOG"
)

var (
	logger = logrus.New()
)

func init() {
	envLevel, _ := os.LookupEnv(LogLevel)
	if envLevel == "" {
		envLevel = "info"
	}
	level := logLevel(envLevel)
	logger.SetLevel(level)
	formatter := &logrus.TextFormatter{
		FullTimestamp:          false,
		DisableLevelTruncation: true,
	}
	logger.SetFormatter(formatter)

}

func logLevel(lvl string) logrus.Level {

	switch lvl {
	case "trace":
		return logrus.TraceLevel
	case "debug":
		return logrus.DebugLevel
	case "info":
		return logrus.InfoLevel
	case "warn":
		return logrus.WarnLevel
	case "error":
		return logrus.ErrorLevel
	case "fatal":
		return logrus.FatalLevel
	default:
		panic(fmt.Sprintf("the specified %s log level is not supported. Use [trace|debug|info|warn|error|fatal]", lvl))
	}
}

func Info(values ...interface{}) {
	logger.Info(values...)
}
func Infof(fmt string, values ...interface{}) {
	logger.Infof(fmt, values...)
}
func Infoln(values ...interface{}) {
	logger.Infoln(values...)
}
func Debugf(fmt string, values ...interface{}) {
	logger.Debugf(fmt, values...)
}
func Debug(values ...interface{}) {
	logger.Debug(values...)
}
func Tracef(fmt string, values ...interface{}) {
	logger.Tracef(fmt, values...)
}
func Trace(values ...interface{}) {
	logger.Trace(values...)
}
func Warnf(fmt string, values ...interface{}) {
	logger.Warnf(fmt, values...)
}
func Fatalf(fmt string, values ...interface{}) {
	logger.Fatalf(fmt, values...)
}
func Errorf(fmt string, values ...interface{}) {
	logger.Errorf(fmt, values...)
}
func Error(values ...interface{}) {
	logger.Error(values...)
}
func Fatal(values ...interface{}) {
	logger.Fatal(values...)
}
func IsTrace() bool {
	return logger.IsLevelEnabled(logrus.TraceLevel)
}
