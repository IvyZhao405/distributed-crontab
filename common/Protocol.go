package common

import "encoding/json"

type Job struct {
	Name string `json:"name"` //task job
	Command string `json:"command"`  //shell command
	CronExpr string `json:"cronExpr"` // cron expression
}

//HTTP response
type Response struct {
	Errno int `json:"errno"`
	Msg string `json:"msg"`
	Data interface{} `json:"data"`
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