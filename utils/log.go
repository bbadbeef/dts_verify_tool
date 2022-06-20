package utils

import (
	"bytes"
	"fmt"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/sirupsen/logrus"
	"io"
	"path"
	"runtime"
	"strings"
	"time"
)

var (
	// GlobalLogger ...
	GlobalLogger *logrus.Logger
)

// SetGlobalLogger ...
func SetGlobalLogger(l *logrus.Logger) {
	GlobalLogger = l
}

// NewLogger ...
func NewLogger(out io.Writer) *logrus.Logger {
	l := logrus.New()
	l.SetFormatter(&Formatter{})
	l.SetLevel(logrus.InfoLevel)
	l.SetOutput(out)
	return l
}

// Formatter ...
type Formatter struct {
}

// Format ...
func (m *Formatter) Format(entry *logrus.Entry) ([]byte, error) {
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	timestamp := entry.Time.Format("2006-01-02 15:04:05.000")

	var file string
	var line int
	var ok bool
	var pc uintptr

	depth := 1
	for {
		pc, file, line, ok = runtime.Caller(depth)
		if strings.HasPrefix(path.Base(runtime.FuncForPC(pc).Name()), "logrus") {
			depth++
		} else {
			break
		}
	}

	if !ok {
		file = "unknown-file"
		line = 0
	}

	var newLog string
	newLog = fmt.Sprintf("[%s] [%s] [%s:%d] [%s] %s\n", timestamp, entry.Level, path.Base(file), line,
		strings.TrimLeft(path.Ext(runtime.FuncForPC(pc).Name()), "."), entry.Message)

	b.WriteString(newLog)
	return b.Bytes(), nil
}

// GetRotateWriter ...
func GetRotateWriter(logPath string) (io.Writer, error) {
	return rotatelogs.New(
		logPath+".%Y%m%d",
		rotatelogs.WithLinkName(logPath),
		rotatelogs.WithMaxAge(time.Duration(3)*24*time.Hour),
		rotatelogs.WithRotationTime(24*time.Hour),
		rotatelogs.WithRotationSize(800*1024*1024),
	)
}
