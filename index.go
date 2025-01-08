package main

import (
	"fmt"

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
	JdbcUrl    string
	Statements []string
	Tables     []rdbms.ITable
}

type Options struct {
	LocalStoragePath string
	LogFilePath      string
	DBConfigs        []DBConfig
}

func LTS_Start(options *Options) {
	fmt.Println("Start")
}

func LTS_Stop() {
	fmt.Println("Stop")
}
