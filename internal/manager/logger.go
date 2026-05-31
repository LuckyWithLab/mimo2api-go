package manager

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"mimo2api/internal/config"
)

var (
	managerLoggerOnce sync.Once
	managerLogger     *log.Logger
)

func getManagerLogger() *log.Logger {
	managerLoggerOnce.Do(func() {
		writer := io.Writer(os.Stderr)
		logPath := strings.TrimSpace(config.ManagerLogPath)
		if logPath == "" {
			logPath = filepath.Join("logs", "manager.log")
		}

		if err := os.MkdirAll(filepath.Dir(logPath), 0755); err != nil {
			log.Printf("failed to create manager log dir for %s: %v", logPath, err)
		} else if file, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644); err != nil {
			log.Printf("failed to open manager log file %s: %v", logPath, err)
		} else {
			writer = io.MultiWriter(os.Stderr, file)
		}

		managerLogger = log.New(writer, "", log.LstdFlags)
	})
	return managerLogger
}

func managerLogf(format string, args ...interface{}) {
	getManagerLogger().Printf(format, args...)
}
