package DataBase

import (
	"KeyValueEngine/Core/Structures/Cache"
	"KeyValueEngine/Core/Structures/Config"
	"KeyValueEngine/Core/Structures/CountMinSketch"
	"KeyValueEngine/Core/Structures/HyperLogLog"
	"KeyValueEngine/Core/Structures/LSMTree"
	"KeyValueEngine/Core/Structures/Memtable"
	"KeyValueEngine/Core/Structures/WAL"
)

type DateBase struct {
	wal WAL.WAL
	lsm LSMTree.LSM
	cache Cache.CacheLRU
	hll HyperLogLog.HLL
	cms CountMinSketch.CountMinSketch
}

func InitDataBase() (*DateBase){
	var config Config.Config
	config.LoadConfig()

	wal := WAL.InitWAL(uint8(config.MaxWALSize), uint8(config.DeleteWALSize))
	memtable := Memtable.InitMemtable(config.MemtableCapacity, config.MemtableTreshold)
	lsm := LSMTree.InitLSM(*memtable, uint16(config.LsmMaxLevel), uint16(config.LsmMaxIndex))
	cache := Cache.InitCache(config.CacheSize)

	return &DateBase{
		wal: *wal,
		lsm: *lsm,
		cache: *cache,
	}
}

