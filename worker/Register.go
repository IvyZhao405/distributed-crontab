package worker

import (
	"context"
	"distributed-crontab/common"
	"github.com/coreos/etcd/clientv3"
	"net"
	"time"
)

//register node to etcd: cron/workers/ip-address
type Register struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease

	localIP string //local ip
}
var (
	G_register *Register
)

//get current machine's ip
func getLocalIP()(ipv4 string, err error) {
	var (
		addrs []net.Addr
		addr net.Addr
		ipNet *net.IPNet
		isIpNet bool
	)
	//get all network interface
	if addrs, err = net.InterfaceAddrs(); err != nil {
		return
	}
	//get first none localhost IP address
	for _, addr = range addrs {
		//ipv4, ipv6
		//this network address is ip address
		if ipNet, isIpNet = addr.(*net.IPNet); isIpNet && !ipNet.IP.IsLoopback() {
			//skip ipv6
			if ipNet.IP.To4() != nil {
				ipv4 = ipNet.IP.String() //192.168.1.1
				return
			}
		}
	}
	err = common.ERR_NO_LOCAL_IP_FOUND
	return
}

func(register *Register) keepOnline(){
	var (
		regKey string
		leaseGrantResp *clientv3.LeaseGrantResponse
		err error
		keepAliveChan <- chan *clientv3.LeaseKeepAliveResponse
		keepAliveResp *clientv3.LeaseKeepAliveResponse
		cancelCtx context.Context
		cancelFunc context.CancelFunc
	)
	for {
		//register path
		regKey = common.JOB_WORKER_DIR + register.localIP

		cancelFunc = nil
		//create lease
		if leaseGrantResp, err = register.lease.Grant(context.TODO(), 10); err != nil {
			goto RETRY
		}

		cancelCtx, cancelFunc = context.WithCancel(context.TODO())

		//auto renew lease
		if keepAliveChan, err = register.lease.KeepAlive(cancelCtx, leaseGrantResp.ID); err != nil {
			goto RETRY
		}

		//register to etcd
		if _, err = register.kv.Put(cancelCtx, regKey, "", clientv3.WithLease(leaseGrantResp.ID)); err != nil {
			goto RETRY
		}

		for {
			select {
			case keepAliveResp = <- keepAliveChan:
				if keepAliveResp == nil {
					goto RETRY
				}
			}
		}
		RETRY:
		time.Sleep(1 * time.Second)
		if cancelFunc != nil {
			cancelFunc()
		}
	}

}
func InitRegister()(err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
		localIp string
	)

	config = clientv3.Config{
		Endpoints: G_config.EtcdEndpoints, //cluster address
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) *time.Millisecond, //connection timeout
	}

	//establish connection
	if client, err = clientv3.New(config); err != nil {
		return
	}
	//current machine ip
	if localIp, err = getLocalIP(); err != nil {
		return
	}
	//get KV and Lease API
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	G_register = &Register{
		client: client,
		kv : kv,
		lease: lease,
		localIP: localIp,
	}

	//register service
	go G_register.keepOnline()
	return
}