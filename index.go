package lts

import (
	"github.com/sssxyd/go-lts-core/rdbms"
	"gopkg.in/natefinch/lumberjack.v2"
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
	LogMaxMegaBytes int
	LogMaxAgeDay    int
	LogCompress     bool
	LogStdOut       bool
	StorageFilePath string
	DBConfigs       []DBConfig
}

var (
	localStorage *LocalStorage
	logger       *lumberjack.Logger
)

func Initialize(options *Options) {

	// 初始化日志
	logger = initialize_lumberjack_logger(options.LogFilePath, options.LogMaxMegaBytes, options.LogMaxAgeDay, options.LogCompress, options.LogStdOut)

	// 初始化本地存储
	if options.StorageFilePath != "" {
		localStorage = initialize_sqlite_local_storage(options.StorageFilePath)
	}

	// 初始化数据库
	for _, dbConfig := range options.DBConfigs {
		_, err := rdbms.NewDataSource(dbConfig.Id, dbConfig.DBUrl, dbConfig.Statements, dbConfig.Tables)
		if err != nil {
			panic(err)
		}
	}
}

func Dispose() {
	// 关闭日志文件
	if logger != nil {
		logger.Close()
	}

	// 关闭本地存储, 本地存储由数据库实现，所以关闭数据库即可
	// 关闭数据库
	rdbms.Close()
}

func Storage() *LocalStorage {
	return localStorage
}

func GetDao() rdbms.IDao {
	return NewDao("")
}

func NewDao(dataSourceId string) rdbms.IDao {
	ds := rdbms.GetDataSource(dataSourceId)
	if ds == nil {
		return nil
	}
	return ds.NewDao()
}
