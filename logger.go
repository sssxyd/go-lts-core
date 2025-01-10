package lts

import (
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/sssxyd/go-lts-core/basic"
	"gopkg.in/natefinch/lumberjack.v2"
)

func initialize_lumberjack_logger(logFilePath string, maxMegaBytes int, maxAgeDay int, compress bool, stdOut bool) *lumberjack.Logger {

	// // 检查日志目录是否存在
	logDir := filepath.Dir(logFilePath)
	basic.TouchDir(logDir)

	if maxMegaBytes <= 0 {
		maxMegaBytes = 100
	}
	if maxAgeDay < 0 {
		maxAgeDay = 0
	}

	// 配置 lumberjack 日志文件管理
	logger := &lumberjack.Logger{
		Filename:  logFilePath,
		MaxSize:   maxMegaBytes, // 每个日志文件最大大小（单位：MB）
		MaxAge:    maxAgeDay,    // 日志文件保留天数
		Compress:  compress,     // 自动压缩旧日志文件
		LocalTime: true,         // 使用本地时间
	}

	if stdOut {
		//设置 MultiWriter，同时输出到文件和 stdout
		mw := io.MultiWriter(os.Stdout, logger)
		log.SetOutput(mw)
	} else {
		log.SetOutput(logger)
	}

	log.SetFlags(log.LstdFlags | log.Lshortfile) // 时间戳 + 文件名行号

	return logger
}
