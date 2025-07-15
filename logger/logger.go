package logger

import (
	"fmt"
	"log"
	"os"

	"it.smaso/tgfuse/configs"
)

type Level = string

const (
	Info  Level = "info"
	Warn  Level = "warn"
	Error Level = "error"
)

type Logger struct {
	file *os.File
}

var (
	logChan   = make(chan string, 100)
	closeChan = make(chan any)
	instance  *Logger
)

func New() *Logger {
	if instance == nil {
		instance = &Logger{}
		instance.initLogger()
	}
	return instance
}

func Log(level Level, msg string) {
	New().Log(level, msg)
}

func LogInfo(msg string) {
	New().Log(Info, msg)
}

func LogWarn(msg string) {
	New().Log(Warn, msg)
}

func LogErr(msg string) {
	New().Log(Error, msg)
}

func (l *Logger) initLogger() {
	file, err := os.OpenFile(configs.LOG_FILE, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o666)
	if err != nil {
		file, err = os.Create(configs.LOG_FILE)
		if err != nil {
			log.Fatalf("Failed to create log file -> %s", err.Error())
		}
	}
	// log.SetOutput(file)
	l.file = file
	go asyncLogWorker()
}

func (l *Logger) Close() {
	closeChan <- 1
	if l.file != nil {
		l.file.Close()
	}
}

func asyncLogWorker() {
	for {
		select {
		case msg := <-logChan:
			log.Println(msg)
		case <-closeChan:
		}
	}
}

func (l *Logger) Log(level Level, message string) {
	select {
	case logChan <- fmt.Sprintf("[%s] %s", level, message):
	default:
		log.Println("Log channel is full, dropping message")
	}
}
