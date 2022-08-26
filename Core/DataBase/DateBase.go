package DataBase

import (
	"KeyValueEngine/Core/Structures/Cache"
	"KeyValueEngine/Core/Structures/CountMinSketch"
	"KeyValueEngine/Core/Structures/HyperLogLog"
	"KeyValueEngine/Core/Structures/LSMTree"
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
	return &DateBase{}
}
