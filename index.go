package lts

import (
	"os"

	"github.com/sssxyd/go-lts-core/basic"
	"github.com/sssxyd/go-lts-core/rdbms"
)

type ApiResult struct {
	Code   int         `json:"code"`
	Msg    string      `json:"msg"`
	Result interface{} `json:"result"`
	Micros int         `json:"micros"`
}

type DBConfig struct {
	Id         string
	DBUrl      string
	Statements []string
	Tables     []rdbms.ITable
}

type Options struct {
	LogFilePath     string
	LogStdOut       bool
	StorageFilePath string
	DBConfigs       []DBConfig
}

var (
	localStorage *LocalStorage
	appLogFile   *os.File
)

func Start(options *Options) {

	// 初始化日志
	app_log, err := basic.InitializeLogFile(options.LogFilePath, options.LogStdOut)
	if err != nil {
		panic(err)
	}
	appLogFile = app_log

	// 初始化本地存储
	if options.LogFilePath != "" {
		localStorage = init_local_storage(options.StorageFilePath)
	}

	// 初始化数据库
	for _, dbConfig := range options.DBConfigs {
		_, err := rdbms.NewDataSource(dbConfig.Id, dbConfig.DBUrl, dbConfig.Statements, dbConfig.Tables)
		if err != nil {
			panic(err)
		}
	}
}

func Stop() {
	// 关闭日志文件
	if appLogFile != nil {
		appLogFile.Close()
	}

	// 关闭本地存储, 本地存储由数据库实现，所以关闭数据库即可
	// 关闭数据库
	rdbms.Close()
}

func Storage() *LocalStorage {
	return localStorage
}
