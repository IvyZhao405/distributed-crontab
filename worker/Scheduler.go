package worker

import (
	"distributed-crontab/common"
	"fmt"
	"time"
)

type Scheduler struct {
	jobEventChan chan *common.JobEvent // etcd job event array
	jobPlanTable map[string]*common.JobSchedulePlan //job scheduler plan table
	jobExecutingTable map[string]*common.JobExecuteInfo //job executing table
	jobResultChan chan *common.JobExecuteResult //job result array
}

var (
	G_scheduler *Scheduler
)
//handle job execution event
func (scheduler *Scheduler) handleJobEvent(jobEvent *common.JobEvent) {
	var (
		jobSchedulePlan *common.JobSchedulePlan
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting bool
		jobExisted bool
		err error
	)
	switch jobEvent.EventType {
	case common.JOB_EVENT_SAVE: //save job event
		if jobSchedulePlan, err = common.BuildJobSchedulePlan(jobEvent.Job); err != nil {
			return
		}
		scheduler.jobPlanTable[jobEvent.Job.Name] = jobSchedulePlan
	case common.JOB_EVENT_DELETE: //delete job event
		if jobSchedulePlan, jobExisted = scheduler.jobPlanTable[jobEvent.Job.Name]; jobExisted {
			delete(scheduler.jobPlanTable, jobEvent.Job.Name)
		}
	case common.JOB_EVENT_KILL: //force kill job event
		//1. check if job is already executing, if it is kill
		fmt.Println("new kill event, handling")
		if jobExecuteInfo, jobExecuting = scheduler.jobExecutingTable[jobEvent.Job.Name]; jobExecuting {
			jobExecuteInfo.CancelFunc() //use kill command to kill job
		}
	}
}

//try execute job
func (scheduler *Scheduler) TryStartJob(jobPlan *common.JobSchedulePlan) {
	//schedule and executing are different
	var (
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting bool
	)
	//scheduled job might run for a while, but we schedule it every second, in this case we don't execute again while still executing

	//if job is executing, skip
	if jobExecuteInfo, jobExecuting = scheduler.jobExecutingTable[jobPlan.Job.Name]; jobExecuting {
		//fmt.Println("Still executing, skip Execution:", jobPlan.Job.Name)
		return
	}
	jobExecuteInfo = common.BuildJobExecuteInfo(jobPlan)

	//save execution status
	scheduler.jobExecutingTable[jobPlan.Job.Name] = jobExecuteInfo
	//execute job(shell command)
	fmt.Println("Executing Job:", jobPlan.Job.Name, jobExecuteInfo.PlanTime, jobExecuteInfo.RealTime)
	G_executor.ExecuteJob(jobExecuteInfo)
}
//recalculate schedule
func (scheduler *Scheduler) TrySchedule() (scheduleAfter time.Duration){
	var (
		jobPlan *common.JobSchedulePlan
		now time.Time
		nearTime *time.Time
	)
	//if scheduler is empty.
	if len(scheduler.jobPlanTable) == 0 {
		scheduleAfter = 1 * time.Second
		return
	}
	//get current time
	now = time.Now()
	//1. iteration all jobs
	for _, jobPlan = range scheduler.jobPlanTable {
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now) {
			scheduler.TryStartJob(jobPlan)
			jobPlan.NextTime = jobPlan.Expr.Next(now) // next execution time
		}
		//calculate most recent expiring job's time
		if nearTime == nil || jobPlan.NextTime.Before(*nearTime){
			nearTime = &jobPlan.NextTime
		}
	}
	//nearest schedule time for a job
	scheduleAfter = (*nearTime).Sub(now) //how much time need to sleep
	return
}

//handle job result
func (scheduler *Scheduler) handleJobResult(result *common.JobExecuteResult) {
	var (
		jobLog *common.JobLog
	)
	//delete job executing status
	delete(scheduler.jobExecutingTable, result.ExecuteInfo.Job.Name)
	//create job execution log
	if result.Err != common.ERR_LOCK_ALREADY_REQUIRED {
		jobLog = &common.JobLog{
			JobName: result.ExecuteInfo.Job.Name,
			Command: result.ExecuteInfo.Job.Command,
			Output: string(result.Output),
			PlanTime: result.ExecuteInfo.PlanTime.UnixNano()/1000/1000,
			ScheduleTime: result.ExecuteInfo.RealTime.UnixNano()/1000/1000,
			StartTime: result.StartTime.UnixNano()/1000/1000,
			EndTime: result.EndTime.UnixNano()/1000/1000,
		}
		if result.Err != nil {
			jobLog.Err = result.Err.Error()
		} else {
			jobLog.Err = ""
		}
		//TODO: save result to MongoDB
	}
	fmt.Println("Job finished executing:", result.ExecuteInfo.Job.Name, string(result.Output), result.Err)
}


//scheduler coroutine
func (scheduler *Scheduler) scheduleLoop() {
	var (
		jobEvent *common.JobEvent
		scheduleAfter time.Duration
		scheduleTimer *time.Timer
		jobResult *common.JobExecuteResult
	)
	//initialize scheduler (return 1 s)
	scheduleAfter = scheduler.TrySchedule()

	//job schedule timer for next time
	scheduleTimer = time.NewTimer(scheduleAfter)
	for {
		select {
		case jobEvent = <- scheduler.jobEventChan: //watch job change event
			scheduler.handleJobEvent(jobEvent)
		case <- scheduleTimer.C: // most recent schedule expired
		case jobResult = <- scheduler.jobResultChan: //listen to execution result
			scheduler.handleJobResult(jobResult)
		}
		//scan job list again, then execute job and update list
		scheduleAfter = scheduler.TrySchedule()
		//reset sleep time
		scheduleTimer.Reset(scheduleAfter)
	}
}
//push job change event
func (scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.jobEventChan <- jobEvent
}

//init scheduler
func InitScheduler() (err error){
	G_scheduler = &Scheduler{
		jobEventChan: make(chan *common.JobEvent, 1000),
		jobPlanTable: make(map[string]*common.JobSchedulePlan),
		jobExecutingTable: make(map[string]*common.JobExecuteInfo),
		jobResultChan: make(chan *common.JobExecuteResult, 1000),
	}
	//start scheduler goroutine
	go G_scheduler.scheduleLoop()
	return
}
//push result to scheduler channel
func(scheduler *Scheduler) PushJobResult(jobResult *common.JobExecuteResult) {
	scheduler.jobResultChan <- jobResult
}