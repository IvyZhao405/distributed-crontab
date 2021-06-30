package master

import (
	"context"
	"distributed-crontab/common"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"time"
)

// /cron/workers/xx all registereed workers
type WorkerMgr struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
}

var (
	G_workerMgr *WorkerMgr
)

//get online worker list
func (workerMgr *WorkerMgr) ListWorkers()(workerArr []string, err error) {
	var (
		getResp *clientv3.GetResponse
		kv *mvccpb.KeyValue
		workerIP string
	)
	//initialize array
	workerArr = make([]string, 0)

	//get all kv in the directory
	if getResp, err = workerMgr.kv.Get(context.TODO(), common.JOB_WORKER_DIR, clientv3.WithPrefix()); err != nil {
		return
	}

	//get available nodes' ips
	for _, kv = range getResp.Kvs {
		//kv.Key: /cron/workers/192.128.2.1
		workerIP = common.ExtractWorkerIP(string(kv.Key))
		workerArr = append(workerArr, workerIP)
	}
	return
}


func InitWorkerMgr() (err error) {
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

	G_workerMgr = &WorkerMgr{
		client: client,
		kv: kv,
		lease: lease,

	}
	return
}