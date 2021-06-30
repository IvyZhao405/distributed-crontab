package worker

import (
	"context"
	"distributed-crontab/common"
	"github.com/coreos/etcd/clientv3"
)

//distributed lock(TXN trasaction)
type JobLock struct {
	//etcd client
	kv clientv3.KV
	lease clientv3.Lease

	jobName string //job name
	cancelFunc context.CancelFunc //used for cancel auto lease renew
	leaseId clientv3.LeaseID //leaseID
	isLocked bool //whether lock successfully
}

func InitJobLock(jobName string, kv clientv3.KV, lease clientv3.Lease) (jobLock *JobLock) {
	jobLock = &JobLock{
		kv: kv,
		lease: lease,
		jobName: jobName,
	}
	return
}

func(jobLock *JobLock) TryLock() (err error) {
	var (
		leaseGrantResp *clientv3.LeaseGrantResponse
		cancelCtx context.Context
		cancelFunc context.CancelFunc
		leaseId clientv3.LeaseID
		keepRespChan <- chan *clientv3.LeaseKeepAliveResponse
		txn clientv3.Txn
		lockKey string
		txnResp *clientv3.TxnResponse
	)
	//1. create lease (5 second, so if node goes down, lease would be released
	if leaseGrantResp, err = jobLock.lease.Grant(context.TODO(), 5); err != nil {
		return
	}
	//context used for cancel auto lease renew
	cancelCtx, cancelFunc = context.WithCancel(context.TODO())
	//lease id
	leaseId = leaseGrantResp.ID

	//2. auto renew lease
	if keepRespChan, err = jobLock.lease.KeepAlive(cancelCtx, leaseId); err != nil {
		goto FAIL
	}
	//3. auto renew lease goroutine
	go func() {
		var(
			keepResp *clientv3.LeaseKeepAliveResponse
		)
		for {
			select {
			case keepResp = <- keepRespChan: //auto renew lease response
				if keepResp == nil {
					goto END
				}
			}
		}
		END:
	}()
	//4. create txn transaction
	txn = jobLock.kv.Txn(context.TODO())
	lockKey = common.JOB_LOCK_DIR + jobLock.jobName
	//5, fight for lock using txn transaction
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, "",clientv3.WithLease(leaseId))).
		Else(clientv3.OpGet(lockKey))

	//submit transaction
	if txnResp, err = txn.Commit(); err != nil {
		goto FAIL
	}
	//6, success return, fail return lease
	if !txnResp.Succeeded { //lock has been taken, fail
		err = common.ERR_LOCK_ALREADY_REQUIRED
		goto FAIL
	}
	//success
	jobLock.leaseId = leaseId
	jobLock.cancelFunc = cancelFunc
	jobLock.isLocked = true
	return

FAIL:
	cancelFunc()//cancel auto renew lease
	jobLock.lease.Revoke(context.TODO(), leaseId)//return lease
	return
}

//return lease
func(jobLock *JobLock) Unlock(){
	if jobLock.isLocked {
		jobLock.cancelFunc() //cancel auto lease renew
		jobLock.lease.Revoke(context.TODO(), jobLock.leaseId)//return lease
	}
}