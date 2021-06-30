package worker

import (
	"encoding/json"
	"io/ioutil"
)

//app config params
type Config struct {
	EtcdEndpoints []string `json:"etcdEndpoints"`
	EtcdDialTimeout int `json:"etcdDialTimeout"`
	MongodbUri string `json:"mongodbUri"`
	MongodbConnectTimeout int `json:"mongodbConnectTimeout"`
	JobLogBatchSize int `json:"jobLogBatchSize"`
	JobLogCommitTimeout int `json:"jobLogCommitTimeout"`
}

var (
	//singleton for config
	G_config *Config
)

//load json config file
func InitConfig(filename string) (err error){
	var (
		content []byte
		conf Config
	)

	//1. read in config file
	if content, err = ioutil.ReadFile(filename); err != nil {
		return
	}

	//2. unserialize json
	if err = json.Unmarshal(content, &conf); err != nil {
		return
	}

	//3.initialize singleton
	G_config = &conf

	return
}