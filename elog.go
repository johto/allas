package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

var elog *Logger

type LogSeverity int

const (
	DEBUG LogSeverity = iota
	LOG
	WARNING
	ERROR
	FATAL
	PANIC
)

func (s LogSeverity) String() string {
	switch s {
	case DEBUG:
		return "DEBUG"
	case LOG:
		return "LOG"
	case WARNING:
		return "WARNING"
	case ERROR:
		return "ERROR"
	case FATAL:
		return "FATAL"
	case PANIC:
		return "PANIC"
	default:
		panic("unknown severity")
	}
}

type Logger struct {
	elog *log.Logger
}

func (l *Logger) Print(severity LogSeverity, output string) {
	l.elog.Print(time.Now().Format("2006-01-02 15:04:05.000 -0700") + " " + severity.String() + ":  " + output)

	switch severity {
	case FATAL:
		os.Exit(1)
	case PANIC:
		panic("Logger.Panic")

	default:
	}
}
func (l *Logger) Printf(severity LogSeverity, format string, v ...interface{}) {
	l.Print(severity, fmt.Sprintf(format, v...))
}
func (l *Logger) Debug(output string) {
	l.Print(DEBUG, output)
}
func (l *Logger) Debugf(format string, v ...interface{}) {
	l.Printf(DEBUG, format, v...)
}
func (l *Logger) Log(output string) {
	l.Print(LOG, output)
}
func (l *Logger) Logf(format string, v ...interface{}) {
	l.Printf(LOG, format, v...)
}
func (l *Logger) Warning(output string) {
	l.Print(WARNING, output)
}
func (l *Logger) Warningf(format string, v ...interface{}) {
	l.Printf(WARNING, format, v...)
}
func (l *Logger) Error(output string) {
	l.Print(ERROR, output)
}
func (l *Logger) Errorf(format string, v ...interface{}) {
	l.Printf(ERROR, format, v...)
}
func (l *Logger) Fatal(output string) {
	l.Print(FATAL, output)
}
func (l *Logger) Fatalf(format string, v ...interface{}) {
	l.Printf(FATAL, format, v...)
}
func (l *Logger) Panic(output string) {
	l.Print(PANIC, output)
}
func (l *Logger) Panicf(format string, v ...interface{}) {
	l.Printf(PANIC, format, v...)
}

func InitErrorLog(w io.Writer) {
	if elog != nil {
		panic("double init")
	}
	elog = &Logger{
		elog: log.New(w, "", 0),
	}
}
