package main

import (
	"distributed-crontab/worker"
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
	//worker -config ./worker.json
	//worker -h
	flag.StringVar(&confFile, "config", "./worker.json", "specify worker.json as config file")
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
	if err = worker.InitConfig(confFile); err != nil {
		goto ERR
	}

	//start executor
	if err = worker.InitExecutor(); err != nil {
		goto ERR
	}

	//start job scheduler
	if err = worker.InitScheduler(); err != nil {
		goto ERR
	}
	//initialize worker manager
	if err = worker.InitJobMgr(); err != nil {
		goto ERR
	}
	//normal exit
	for {
		time.Sleep(1 * time.Second)
	}
	return
ERR:
	fmt.Println(err)
}

