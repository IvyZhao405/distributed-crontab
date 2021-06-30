package common

import (
	"context"
	"encoding/json"
	"github.com/gorhill/cronexpr"
	"strings"
	"time"
)

type Job struct {
	Name string `json:"name"` //task job
	Command string `json:"command"`  //shell command
	CronExpr string `json:"cronExpr"` // cron expression
}

//job schedule plan
type JobSchedulePlan struct {
	Job *Job // job schedule information
	Expr *cronexpr.Expression //parsed cron expression
	NextTime time.Time // next execution time
}
//executing job info
type JobExecuteInfo struct {
	Job *Job // job info
	PlanTime time.Time //calculated schedule time
	RealTime time.Time //actual scheduled time
	CancelCtx context.Context //context for canceling job
	CancelFunc context.CancelFunc // Cancel func for canceling job
}

//HTTP response
type Response struct {
	Errno int `json:"errno"`
	Msg string `json:"msg"`
	Data interface{} `json:"data"`
}

//change event
type JobEvent struct {
	EventType int  // SAVE, DELETE
	Job *Job
}

//job execution result
type JobExecuteResult struct {
	ExecuteInfo *JobExecuteInfo // Execution status
	Output []byte //shell script output
	Err error // shell error
	StartTime time.Time //job start time
	EndTime time.Time // job end time
}

//job execution log
type JobLog struct {
	JobName string `json:"jobName" bson:"jobName"` //job name
	Command string `json:"command" bson:"command"` //shell command
	Err string `json:"err" bson:"err"` // error
	Output string `json:"output" bson:"output"`//script output
	PlanTime int64 `json:"planTime" bson:"planTime"` //plan start time
	ScheduleTime int64 `json:"scheduleTime" bson:"scheduleTime"` //actual scheduled time
	StartTime int64 `json:"startTime" bson:"startTime"` //job execution start time
	EndTime int64 `json:"endTime" bson:"endTime"` //job execution end time
}

type LogBatch struct {
	Logs []interface{} //multiple logs
}

//mongo log filter condition
type JobLogFilter struct {
	JobName string `bson:"jobName"`
}

//job log sorting rule
type SortLogByStartTime struct {
	SortOrder int `bson:"startTime"` //{startTime:-1}
}

//resposne method
func BuildResponse(errno int, msg string, data interface{}) (resp []byte, err error){
	//1. define a response
	var (
		response Response
	)
	response.Errno = errno
	response.Msg = msg
	response.Data = data

	//2. serialize json
	resp, err = json.Marshal(response)
	return
}

//deserialize job
func UnpackJob(value []byte) (ret *Job, err error){
	var (
		job *Job
	)

	job = &Job{}
	if err = json.Unmarshal(value, job); err != nil {
		return
	}
	ret = job
	return
}

//parse job name from etcd job key
///cron/jobs/job10 remove /cron/jobs/
func ExtractJobName(jobKey string) (string) {
	return strings.TrimPrefix(jobKey, JOB_SAVE_DIR)
}

//parse job name from etcd killer key
///cron/killer/job10 remove /cron/killer/
func ExtractKillerName(killerKey string) (string) {
	return strings.TrimPrefix(killerKey, JOB_KILLER_DIR)
}

//job change even 2 types: 1. update event. 2. delete event
func BuildJobEvent(eventType int, job *Job) (jobEvent *JobEvent) {
	return &JobEvent{
		EventType: eventType,
		Job: job,
	}
}

//build job execution plan
func BuildJobSchedulePlan(job *Job)(jobSchedulePlan *JobSchedulePlan, err error){
	var (
		expr *cronexpr.Expression
	)

	//parse job's cron expression
	if expr, err = cronexpr.Parse(job.CronExpr); err != nil {
		return
	}
	//create job schedule plan
	jobSchedulePlan = &JobSchedulePlan{
		Job: job,
		Expr: expr,
		NextTime: expr.Next(time.Now()),
	}
	return
}

func BuildJobExecuteInfo(jobSchedulePlan *JobSchedulePlan) (jobExecuteInfo *JobExecuteInfo) {
	jobExecuteInfo = &JobExecuteInfo{
		Job:jobSchedulePlan.Job,
		PlanTime: jobSchedulePlan.NextTime, //calculated schedule time
		RealTime: time.Now(), //actual scheduled time
	}
	jobExecuteInfo.CancelCtx, jobExecuteInfo.CancelFunc = context.WithCancel(context.TODO())
	return
}

//get worker's ip
func ExtractWorkerIP(regKey string) (string) {
	return strings.TrimPrefix(regKey, JOB_WORKER_DIR)
}
