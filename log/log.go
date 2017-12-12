package log

import (
	"log"
	"os"
)

var defaultLogger ILogger

func init() {
	defaultLogger = &Logger{"~", log.New(os.Stdout, "~", log.Ldate|log.Ltime|log.Lshortfile)}
}

type ILogger interface {
	Info(msg string, v ...interface{})
	Warn(msg string, v ...interface{})
	Debug(msg string, v ...interface{})
	Error(msg string, v ...interface{})
	Fatal(msg string, v ...interface{})
}

type Logger struct {
	prefix string
	log    *log.Logger
}

func (z *Logger) Info(msg string, v ...interface{}) {
	z.log.Printf("INFO "+msg, v...)
}
func (z *Logger) Warn(msg string, v ...interface{}) {
	z.log.Printf("WARN "+msg, v...)
}
func (z *Logger) Debug(msg string, v ...interface{}) {
	z.log.Printf("DEBUG "+msg, v...)
}
func (z *Logger) Error(msg string, v ...interface{}) {
	z.log.Printf("ERROR "+msg, v...)
}
func (z *Logger) Fatal(msg string, v ...interface{}) {
	z.log.Printf("FATAL "+msg, v...)
}

func Info(msg string, v ...interface{}) {
	defaultLogger.Info(msg, v...)
}
func Warn(msg string, v ...interface{}) {
	defaultLogger.Info(msg, v...)
}
func Debug(msg string, v ...interface{}) {
	defaultLogger.Info(msg, v...)
}
func Error(msg string, v ...interface{}) {
	defaultLogger.Error(msg, v...)
}
func Fatal(msg string, v ...interface{}) {
	defaultLogger.Error(msg, v...)
}

func New(name string) ILogger {
	return &Logger{name, log.New(os.Stdout, name+" ", log.Ldate|log.Ltime|log.Lshortfile)}
}
