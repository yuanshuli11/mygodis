package logger

import (
	"fmt"
	"path"
	"time"

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
)

var logClient *logrus.Logger
var logLevels = map[string]logrus.Level{
	"debug": logrus.DebugLevel,
	"warn":  logrus.WarnLevel,
	"info":  logrus.InfoLevel,
}

func init() {
	logClient = logrus.New()
	logClient.Formatter = &logrus.JSONFormatter{}
}


//ConfigLocalFilesystemLogger ConfigLocalFilesystemLogger
func ConfigLocalFilesystemLogger(logPath, loglevel string) {
	logFileName := "logs"
	baseLogPaht := path.Join(logPath, logFileName)
	level, ok := logLevels[loglevel]
	if ok {
		logClient.SetLevel(level)
	} else {
		logClient.SetLevel(logrus.WarnLevel)
	}

	writer, err := rotatelogs.New(
		baseLogPaht+".%Y%m%d%H%M",
		rotatelogs.WithLinkName(baseLogPaht),   // 生成软链，指向最新日志文件
		rotatelogs.WithMaxAge(time.Hour*1200),  // 文件最大保存时间
		rotatelogs.WithRotationTime(time.Hour), // 日志切割时间间隔
	)
	if err != nil {
		logrus.Errorf("config local file system logger error", err)
	}
	lfHook := lfshook.NewHook(lfshook.WriterMap{
		logrus.DebugLevel: writer, // 为不同级别设置不同的输出目的
		logrus.InfoLevel:  writer,
		logrus.WarnLevel:  writer,
		logrus.ErrorLevel: writer,
		logrus.FatalLevel: writer,
		logrus.PanicLevel: writer,
	}, &logrus.JSONFormatter{})
	logClient.AddHook(lfHook)
}

//Warnf Warnf
func Warnf(format string, v ...interface{}) {
	str := fmt.Sprintf(format, v...)

	logClient.WithFields(logrus.Fields{
		"type": "app",
	}).Warn(str)

}


//Infof Infof
func Debugf(format string, v ...interface{}) {
	str := fmt.Sprintf(format, v...)

	logClient.WithFields(logrus.Fields{
		"type": "app",
	}).Debug(str)

}

//Infof Infof
func Infof(format string, v ...interface{}) {
	str := fmt.Sprintf(format, v...)

	logClient.WithFields(logrus.Fields{
		"type": "app",
	}).Info(str)

}



//Errorf Errorf
func Errorf(format string, v ...interface{}) {
	str := fmt.Sprintf(format, v...)

	logClient.WithFields(logrus.Fields{
		"type": "app",
	}).Error(str)
}

