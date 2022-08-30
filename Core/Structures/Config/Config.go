package Config

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Config struct {
	MaxWALSize    int 			 `yaml:"wal_max_size"`
	DeleteWALSize int 			  `yaml:"wal_delete_size"`
	MemtableCapacity int	     `yaml:"mem_cap"`
	MemtableTreshold float64	 `yaml:"mem_treshold"`
	CacheSize int			 	 `yaml:"cache_size"`
	LsmMaxLevel int			 	 `yaml:"lsm_level"`
	LsmMaxIndex int			 	 `yaml:"lsm_index"`
}

func (config *Config) LoadConfig(){
	file, err := ioutil.ReadFile("data/config/config.yaml")
	if err != nil || len(file) == 0 {
		config.MaxWALSize = 9
		config.DeleteWALSize = 5
		config.LsmMaxIndex = 5
		config.LsmMaxLevel = 4
		config.MemtableCapacity = 10
		config.MemtableTreshold = 0.7
		config.CacheSize = 10
	} else {
		err = yaml.Unmarshal(file, config)
		if err != nil {
			panic(err)
		}
	}
}

