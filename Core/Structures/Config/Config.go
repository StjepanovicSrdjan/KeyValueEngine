package Config

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Config struct {
	maxWALSize int           	 `yaml:"wal_max_size"`
	deleteWALSize int		 	 `yaml:"wal_delete_size"`
	memtableCapacity int	     `yaml:"mem_cap"`
	memtableTreshold float64	 `yaml:"mem_treshold"`
	cacheSize int			 	 `yaml:"cache_size"`
	lsmMaxLevel int			 	 `yaml:"lsm_level"`
	lsmMaxIndex int			 	 `yaml:"lsm_index"`
}

func (config *Config) loadConfig(){
	file, err := ioutil.ReadFile("data/config/config.yaml")
	if err != nil || len(file) == 0 {
		config.maxWALSize = 9
		config.deleteWALSize = 5
		config.lsmMaxIndex = 5
		config.lsmMaxLevel = 4
		config.memtableCapacity = 10
		config.memtableTreshold = 0.7
		config.cacheSize = 10
	} else {
		err = yaml.Unmarshal(file, config)
		if err != nil {
			panic(err)
		}
	}
}

