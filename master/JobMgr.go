package master

import (
	"context"
	"distributed-crontab/common"
	"encoding/json"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"time"
)

//
type JobMgr struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
}

var (
	//singleton
	G_jobMgr *JobMgr
)

//
func InitJobMgr()(err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
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

	//initialize singleton
	G_jobMgr = &JobMgr{
		client: client,
		kv: kv,
		lease: lease,
	}
	return
}

//save job
func (jobMgr *JobMgr) SaveJob(job *common.Job) (oldJob *common.Job, err error ){
	//save job to "/cron/jobs/jobname" -> json
	var (
		jobKey string
		jobValue []byte
		putResp *clientv3.PutResponse
		oldJobObj common.Job
	)

	//etcd key
	jobKey = common.JOB_SAVE_DIR + job.Name
	//job info json
	if jobValue, err = json.Marshal(job); err != nil {
		return
	}

	//save to etcd
	if putResp, err = jobMgr.kv.Put(context.TODO(), jobKey, string(jobValue), clientv3.WithPrevKV()); err != nil {
		return
	}
	//if its an update to old value, return old value
	if putResp.PrevKv != nil {
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}
	return
}

//delete job
func (jobMgr *JobMgr) DeleteJob(name string) (oldJob *common.Job, err error) {
	var (
		jobKey string
		delResp *clientv3.DeleteResponse
		oldJobObj common.Job
	)

	//etcd job save key
	jobKey = common.JOB_SAVE_DIR + name

	//delete job from etcd
	if delResp, err = jobMgr.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV()); err != nil {
		return
	}

	//return deleted job's info
	if len(delResp.PrevKvs) != 0 {
		//serialize old job info and return
		if err = json.Unmarshal(delResp.PrevKvs[0].Value, &oldJobObj); err != nil{
			err = nil
			return
		}
		oldJob = &oldJobObj
	}
	return
}

//kill job
func (jobMgr *JobMgr) KillJob(name string) (err error) {
	//insert key=/cron/killer/job name
	var (
		killerKey string
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId clientv3.LeaseID
	)

	//notify worker to kill job
	killerKey = common.JOB_KILLER_DIR + name
	//let worker listen to put notification, create an lease for the key to expire
	if leaseGrantResp, err = jobMgr.lease.Grant(context.TODO(), 1); err != nil {
		return
	}

	//leaseID
	leaseId = leaseGrantResp.ID

	//set up killer notification
	if _, err = jobMgr.kv.Put(context.TODO(), killerKey, "", clientv3.WithLease(leaseId)); err != nil {
		return
	}
	return
}
//list job
func (JobMgr *JobMgr) ListJobs()(jobList []*common.Job, err error) {
	var (
		dirKey string
		getResp *clientv3.GetResponse
		kvPair *mvccpb.KeyValue
		job *common.Job
	)

	//job save directory
	dirKey = common.JOB_SAVE_DIR

	//get all jobs under a directory
	if getResp, err = JobMgr.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix()); err != nil {
		return
	}

	//initialize jobList to len 0
	jobList = make([]*common.Job, 0)
	//len(jobList) == 0

	//iterate all jobs, unserialize
	for _, kvPair= range getResp.Kvs {
		job = &common.Job{}
		if err = json.Unmarshal(kvPair.Value, job); err != nil {
			err = nil
			continue
		}
		jobList = append(jobList, job)
	}
	return
}
