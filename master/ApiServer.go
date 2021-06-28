package master

import (
	"distributed-crontab/common"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"time"
)

//Http server
type ApiServer struct {
	httpServer *http.Server
}

var (
	//Singleton
	G_apiServer *ApiServer
)

//POST job = {"name": "job1", "command": "echo hello", "cronExpr":"* * * * * "}
func handleJobSave(resp http.ResponseWriter, req *http.Request) {
	//job save to ETCD
	var (
		err error
		postJob string
		job common.Job
		oldJob *common.Job
		bytes []byte
	)
	//1. parse POST request form
	if err = req.ParseForm(); err != nil {
		goto ERR
	}
	//2. get job info from form
	postJob = req.PostForm.Get("job")
	//3. unserialize job
	if err = json.Unmarshal([]byte(postJob), &job); err != nil {
		goto ERR
	}
	//4.save to etcd
	if oldJob, err = G_jobMgr.SaveJob(&job); err != nil {
		goto ERR
	}

	//5. return OK response ({"errno":0, "msg": "", "data":{....}})
	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil {
		resp.Write(bytes)
	}
	return
ERR:
	//6. return error response
	if bytes, err = common.BuildResponse(-1,err.Error(), oldJob); err == nil {
		resp.Write(bytes)
	}
}

//delete job path
//POST /job/delete name = job1
func handleJobDelete(resp http.ResponseWriter, req *http.Request) {
	var (
		err error
		name string
		oldJob *common.Job
		bytes []byte
	)
	//POST: a=1&b=2&c=3
	if err = req.ParseForm(); err != nil {
		goto ERR
	}
	//job names needs to be deleted
	name = req.PostForm.Get("name")

	//delete job
	if oldJob, err = G_jobMgr.DeleteJob(name); err != nil {
		goto ERR
	}

	//ok response
	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil {
		resp.Write(bytes)
	}
	return
ERR:
	//error response
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Write(bytes)
	}
}

//force kill
func handleJobKill(resp http.ResponseWriter, req *http.Request) {
	var (
		err error
		name string
		bytes []byte
	)

	if err = req.ParseForm(); err != nil {
		goto ERR
	}

	//job name
	name = req.PostForm.Get("name")

	//kill job
	if err = G_jobMgr.KillJob(name); err != nil {
		goto ERR
	}
	//ok response
	if bytes, err = common.BuildResponse(0, "success", nil); err == nil {
		resp.Write(bytes)
	}
	return
ERR:

	//error response
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Write(bytes)
	}
}

//list all crontab job
func handleJobList(resp http.ResponseWriter, req *http.Request) {
	var (
		jobList []*common.Job
		err error
		bytes []byte
	)
	//get job list
	if jobList, err = G_jobMgr.ListJobs(); err != nil {
		goto ERR
	}
	//return ok response
	if bytes, err = common.BuildResponse(0, "success", jobList); err == nil {
		resp.Write(bytes)
	}
	return
ERR:
	//error response
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Write(bytes)
	}
}

//init http service
func InitApiServer()(err error){
	var (
		mux *http.ServeMux
		listener net.Listener
		httpServer *http.Server
		staticDir http.Dir
		staticHandler http.Handler //static file HTTP handler
	)
	//routing
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDelete)
	mux.HandleFunc("/job/list", handleJobList)
	mux.HandleFunc("/job/kill", handleJobKill)

	//static file directory
	staticDir = http.Dir(G_config.WebRoot)
	staticHandler = http.FileServer(staticDir)
	mux.Handle("/", http.StripPrefix("/",staticHandler)) //./webroot/index.html

	//start TCP listener
	if listener, err = net.Listen("tcp", ":" + strconv.Itoa(G_config.ApiPort)); err != nil {
		return
	}

	//create http service
	httpServer = &http.Server{
		ReadTimeout: time.Duration(G_config.ApiReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(G_config.ApiWriteTimeout) * time.Millisecond,
		Handler: mux,
	}

	//initialize singleton
	G_apiServer = &ApiServer{
		httpServer: httpServer,
	}

	//start HTTP service
	go httpServer.Serve(listener)
	return
}

//{"name":"job1","command":"echo hello","cronExpr":"*/5 * * * * * *"}