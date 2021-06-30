package worker

import (
	"context"
	"distributed-crontab/common"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

//mongoDB log save

type LogSink struct {
	client *mongo.Client
	logCollection *mongo.Collection
	logChan chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}

var (
	//singleton
	G_logSink *LogSink
)

func (logSink *LogSink) saveLogs(batch *common.LogBatch) {
	logSink.logCollection.InsertMany(context.TODO(), batch.Logs)
}

func(logSink *LogSink) writeLoop(){
	var(
		log *common.JobLog
		logBatch *common.LogBatch//current batch
		commitTimer *time.Timer
		timeoutBatch *common.LogBatch //timeout batch
	)
	for {
		select {
		case log = <- logSink.logChan:
			if logBatch == nil {
				logBatch = &common.LogBatch{}
				//if reach a time limit, auto submit batch(1 second)
				commitTimer = time.AfterFunc(time.Duration(G_config.JobLogCommitTimeout) * time.Millisecond,
					func(batch *common.LogBatch) func(){
					return func(){
						logSink.autoCommitChan <- batch
					}
					}(logBatch),
				)
			}
			//append new log to logBatch
			logBatch.Logs = append(logBatch.Logs, log)

			//if batch is fill, then send to mongo
			if len(logBatch.Logs) >= G_config.JobLogBatchSize {
				//send log
				logSink.saveLogs(logBatch)
				//empty logbatch
				logBatch = nil
				//cancel timer
				commitTimer.Stop()
			}
		case timeoutBatch = <- logSink.autoCommitChan: //timeout batch
			//check if timeout batch is still current batch
			if timeoutBatch != logBatch {
				continue //log batch was already submitted
			}
			//write timeout batch to mongo
			logSink.saveLogs(timeoutBatch)
			//empty logbatch
			logBatch = nil
		}
	}
}

func InitLogSink()(err error) {
	var (
		client *mongo.Client
		ctx context.Context
		cancel context.CancelFunc
		url string
	)

	//1. connect to mongodb
	ctx, cancel = context.WithTimeout(context.TODO(), time.Duration(G_config.MongodbConnectTimeout)*time.Millisecond)
	url = "mongodb://localhost:27017"
	defer cancel()

	if client, err = mongo.Connect(ctx,options.Client().ApplyURI(url)); err != nil {
		return
	}

	//choose db and collection
	G_logSink = &LogSink {
		client: client,
		logCollection: client.Database("cron").Collection("log"),
		logChan: make(chan *common.JobLog, 1000),
		autoCommitChan: make(chan *common.LogBatch, 1000),
	}
	//start a mongodb process goroutine
	go G_logSink.writeLoop()
	return
}

//send log
 func(logSink *LogSink) Append(jobLog *common.JobLog) {
 	select {
 	case logSink.logChan <- jobLog:
	default:
		//log chan is full, just return (can be optimized in the future)
	}
 }