package master

import (
	"encoding/json"
	"io/ioutil"
)

//app config params
type Config struct {
	ApiPort int `json:"apiPort"`
	ApiReadTimeout int `json:"apiReadTimeout"`
	ApiWriteTimeout int `json:"apiWriteTimeout"`
	EtcdEndpoints []string `json:"etcdEndpoints"`
	EtcdDialTimeout int `json:"etcdDialTimeout"`
	WebRoot string `json:"webroot"`
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