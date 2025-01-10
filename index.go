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

type LogConfig struct {
	FilePath    string `default:"./logs/app.log"` // 日志文件路径
	MaxMegaByte int    `default:"100"`
	MaxAgeDay   int    `default:"7"`
	Compress    bool   `default:"true"`
	StdOut      bool   `default:"false"`
}

type StorageConfig struct {
	FilePath string `default:"./data/storage.db"` // 本地存储文件路径
}

type Options struct {
	LogConfig     LogConfig
	StorageConfig StorageConfig
	DBConfigs     []DBConfig
}

var (
	localStorage *LocalStorage
	logger       *lumberjack.Logger
)

func Initialize(options *Options) {

	// 初始化日志
	logger = initialize_lumberjack_logger(options.LogConfig.FilePath, options.LogConfig.MaxMegaByte, options.LogConfig.MaxAgeDay, options.LogConfig.Compress, options.LogConfig.StdOut)

	// 初始化本地存储
	if options.StorageConfig.FilePath != "" {
		localStorage = initialize_sqlite_local_storage(options.StorageConfig.FilePath)
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
