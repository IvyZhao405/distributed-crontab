package worker

import (
	"context"
	"distributed-crontab/common"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"time"
)

//
type JobMgr struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
	watcher clientv3.Watcher
}

var (
	//singleton
	G_jobMgr *JobMgr
)

//listen to job change
func (jobMgr *JobMgr) watchJobs() (err error) {
	var (
		getResp *clientv3.GetResponse
		kvpair *mvccpb.KeyValue
		job *common.Job
		watchStartRevision int64
		watchChan clientv3.WatchChan
		watchResp clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobName string
		jobEvent *common.JobEvent
	)
	//1, get all jobs from/cron/jobs/ and get current cluster's revision number
	if getResp, err = jobMgr.kv.Get(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithPrefix()); err != nil {
		return
	}

	for _, kvpair = range getResp.Kvs {
		//deserialize
		if job, err = common.UnpackJob(kvpair.Value); err == nil {
			jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
			//Push the job with schedular (goroutine)
			G_scheduler.PushJobEvent(jobEvent)
		}
	}

	//2, listen in real-time for revision after current revision
	go func() {
		//start from the revision after GET
		watchStartRevision = getResp.Header.Revision + 1
		//start /cron/jobs/ watcher
		watchChan = jobMgr.watcher.Watch(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithRev(watchStartRevision), clientv3.WithPrefix())
		//action for watch change
		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT: //new job save
					if job, err = common.UnpackJob(watchEvent.Kv.Value); err != nil {
						continue
					}
					//create a new event
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
				case mvccpb.DELETE: //job delete
					//Delete /cron/jobs/job10
					jobName = common.ExtractJobName(string(watchEvent.Kv.Key))

					job = &common.Job{Name: jobName}
					//create a delete event
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_DELETE, job)
				}
				G_scheduler.PushJobEvent(jobEvent)
			}
		}
	}()
	return
}

//listen kill job event
func(jobMgr *JobMgr) watchKiller(){
	var (
		watchChan clientv3.WatchChan
		watchResp clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobEvent *common.JobEvent
		jobName string
		job *common.Job
	)
	//listen /cron/killer directory
	//2, listen in real-time for revision after current revision
	go func() {
		//start /cron/killer/ watcher
		watchChan = jobMgr.watcher.Watch(context.TODO(), common.JOB_KILLER_DIR, clientv3.WithPrefix())
		//action for watch change
		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT: //kill job event
					jobName = common.ExtractKillerName(string(watchEvent.Kv.Key)) //cron/killer/jobXXX
					job = &common.Job{Name:jobName}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_KILL, job)
					//event send to scheduler
					G_scheduler.PushJobEvent(jobEvent)
				case mvccpb.DELETE: //job delete

				}
			}
		}
	}()
}

//init job manager
func InitJobMgr()(err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
		watcher clientv3.Watcher
	)

	config = clientv3.Config{
		Endpoints: G_config.EtcdEndpoints, //cluster address
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) *time.Millisecond, //connection timeout
	}

	//establish connection
	if client, err = clientv3.New(config); err != nil {
		return
	}
	//get KV and Lease API
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)

	//initialize singleton
	G_jobMgr = &JobMgr{
		client: client,
		kv: kv,
		lease: lease,
		watcher: watcher,
	}
	//start watching for job needs to be executed
	G_jobMgr.watchJobs()

	//start listening killer jobs
	G_jobMgr.watchKiller()
	return
}

//create job lock
func(jobMgr *JobMgr) CreateJobLock(jobName string)(jobLock *JobLock){
	jobLock = InitJobLock(jobName, jobMgr.kv, jobMgr.lease)
	return
}

