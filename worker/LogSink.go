package worker

import (
	"distributed-crontab/common"
	"go.mongodb.org/mongo-driver/mongo"
)

//mongoDB log save

type LogSink struct {
	client *mongo.Client
	logCollection *mongo.Collection
	logChan chan *common.JobLog
}

var (
	//singleton
	G_logSink *LogSink
)

func InitLog