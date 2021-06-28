package main

import (
	"distributed-crontab/master"
	"flag"
	"fmt"
	"runtime"
	"time"
)

var (
	confFile string // config file path
)
//parse command line parameter
func initArgs(){
	//master -config ./master.json
	flag.StringVar(&confFile, "config", "./master.json", "specify master.json as config file")
	flag.Parse()
}

//initialize # of threads
func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	var (
		err error
	)

	//initialize command line parameter
	initArgs()

	//initialize threads
	initEnv()

	//load config parameters
	if err = master.InitConfig(confFile); err != nil {
		goto ERR
	}

	//init job manager
	if err = master.InitJobMgr(); err != nil {
		goto ERR
	}
	//start Api HTTP service
	if err = master.InitApiServer(); err != nil {
		goto ERR
	}
	//normal exit
	for {
		time.Sleep(1 * time.Second)
	}
	//normal exit
	for {
		time.Sleep(1 * time.Second)
	}
	return
ERR:
	fmt.Println(err)
}

