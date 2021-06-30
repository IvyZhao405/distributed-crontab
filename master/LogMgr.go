package master

import (
	"context"
	"distributed-crontab/common"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type LogMgr struct {
	client *mongo.Client
	logCollection *mongo.Collection
}

var (
	G_logMgr *LogMgr
)

func InitLogMgr() (err error) {
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

	G_logMgr = &LogMgr{
		client: client,
		logCollection: client.Database("cron").Collection("log"),
	}
	return
}
//list job log
func (logMgr *LogMgr) ListLog(name string, skip int, limit int)(logArr []*common.JobLog, err error) {
	var (
		filter *common.JobLogFilter
		logSort *common.SortLogByStartTime
		cursor *mongo.Cursor
		jobLog *common.JobLog
	)
	//corner case in case logs is empty
	logArr = make([]*common.JobLog, 0)
	//filter condition
	filter = &common.JobLogFilter{JobName: name}

	//sort by job starting time
	logSort = &common.SortLogByStartTime{SortOrder: -1}

	if cursor, err = logMgr.logCollection.Find(context.TODO(), filter,
		options.Find().SetSort(logSort),
		options.Find().SetSkip(int64(skip)),
		options.Find().SetLimit(int64(limit))); err != nil {
	}
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		jobLog = &common.JobLog{}

		//deserialize bson
		if err = cursor.Decode(jobLog); err != nil {
			continue //illegal log
		}
		logArr = append(logArr, jobLog)
	}
	return
}