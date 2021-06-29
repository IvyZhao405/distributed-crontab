package worker

import (
	"distributed-crontab/common"
	"math/rand"
	"os/exec"
	"time"
)

//job executor
type Executor struct {

}

var (
	G_executor *Executor
)

//execute job
func(executor *Executor) ExecuteJob(info *common.JobExecuteInfo) {
	go func() {
		var (
			cmd *exec.Cmd
			err error
			output []byte
			result *common.JobExecuteResult
			jobLock *JobLock
		)
		//job result
		result = &common.JobExecuteResult{
			ExecuteInfo: info,
			Output: make([]byte, 0),
		}

		//get distributed lock for this job first
		//initialize lock
		jobLock = G_jobMgr.CreateJobLock(info.Job.Name)
		//job start time
		result.StartTime = time.Now()

		//random sleep(0~1s) because there is discrepancy in workers clock time
		time.Sleep(time.Duration(rand.Intn(1000))*time.Millisecond)

		//put lock
		err = jobLock.TryLock()
		defer jobLock.Unlock()

		if  err != nil { //fail to get lock
			result.Err = err
			result.EndTime = time.Now()
		} else {
			//if succesfully get the lock, reset job start time
			result.StartTime = time.Now()
			//execute shell job
			cmd = exec.CommandContext(info.CancelCtx, "/bin/bash", "-c", info.Job.Command)

			//execute and capture output
			output, err = cmd.CombinedOutput()

			//end time
			result.EndTime = time.Now()
			result.Output = output
			result.Err = err
		}
		//after job is finished executing, return result to Scheduler, then Scheduler can remove job from executing table
		G_scheduler.PushJobResult(result)
	}()
}

//initialize executor
func InitExecutor() (err error) {
	G_executor = &Executor{}
	return
}
