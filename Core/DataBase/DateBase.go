package DataBase

import (
	"KeyValueEngine/Core/Structures/BloomFilter"
	"KeyValueEngine/Core/Structures/Cache"
	"KeyValueEngine/Core/Structures/Config"
	"KeyValueEngine/Core/Structures/CountMinSketch"
	"KeyValueEngine/Core/Structures/Element"
	"KeyValueEngine/Core/Structures/HyperLogLog"
	"KeyValueEngine/Core/Structures/LSMTree"
	"KeyValueEngine/Core/Structures/Memtable"
	"KeyValueEngine/Core/Structures/SSTable"
	"KeyValueEngine/Core/Structures/WAL"
	"fmt"
	"strconv"
)

type DataBase struct {
	wal WAL.WAL
	lsm LSMTree.LSM
	cache Cache.CacheLRU
	hll HyperLogLog.HLL
	cms CountMinSketch.CountMinSketch
}

func InitDataBase() (*DataBase){
	var config Config.Config
	config.LoadConfig()

	wal := WAL.InitWAL(uint8(config.MaxWALSize), uint8(config.DeleteWALSize))
	memtable := Memtable.InitMemtable(config.MemtableCapacity, config.MemtableTreshold)
	lsm := LSMTree.InitLSM(*memtable, uint16(config.LsmMaxLevel), uint16(config.LsmMaxIndex))
	cache := Cache.InitCache(config.CacheSize)

	return &DataBase{
		wal: *wal,
		lsm: *lsm,
		cache: *cache,
		hll: *HyperLogLog.InitHLL(4),
		cms: *CountMinSketch.InitCMS(1, 1),
	}
}

func (db *DataBase) Put(key, value string) {
	valueByte := []byte(value)
	element := Element.InitElement(key, valueByte, 0)

	if !db.wal.Add(key, valueByte){
		panic("WAL ERROR")
	}

	elements := db.lsm.Memtable.Add(*element)
	if elements != nil {

		level := 0
		index := LSMTree.GetLastIndex(0)

		newLevel := strconv.Itoa(level)
		newIndex := strconv.Itoa(index)
		DataFilePath := "data/data/data_" + newLevel + "_" + newIndex + ".bin"
		IndexFilePath := "data/index/index_" + newLevel + "_" + newIndex + ".bin"
		SummeryFilePath := "data/summery/summery_" + newLevel + "_" + newIndex + ".bin"
		FilterFilePath := "data/filter/filter_" + newLevel + "_" + newIndex + ".bin"
		MetadataFilePath := "data/metadata/metadata_" + newLevel + "_" + newIndex + ".bin"
		TOCFilePath := "data/TOC/toc_" + newLevel + "_" + newIndex + ".bin"

		sstable := SSTable.InitSSTable(elements, DataFilePath, IndexFilePath, SummeryFilePath, FilterFilePath, MetadataFilePath,
			TOCFilePath)

		db.lsm.Add(*sstable)

	}
	fmt.Println("finish")
}

func (db *DataBase) Get(key string) (bool, []byte){
	element, found := db.cache.Get(key)
	if found{
		return true, element.Value

	}

	element, err := db.lsm.Memtable.GetElement(key)
	if err == nil {
		if element.Tombstone == 0 {
			db.cache.Add(element)
			return true, element.Value
		}else{
			return false, nil
		}
	}

	latestElement := Element.Element{}
	var foundSS bool
	for i := 0; i < int(db.lsm.MaxLevel); i++ {
		for j := 0; j < len(db.lsm.SSTables[i]); j++ {
			bfPath := db.lsm.SSTables[i][j].FilterFilePath
			bf := BloomFilter.BloomFilter{}
			bf.Decode(bfPath)
			_ = bf.Contains(key)
			/*if !found{
				continue
			}
*/
			currentElement, err := db.lsm.SSTables[i][j].GetElement(key)
			if err != nil {
				continue
			}

			if currentElement.Timestamp > latestElement.Timestamp {
				foundSS = true
				latestElement = *currentElement
			}
		}
	}
	if foundSS {
		if latestElement.Tombstone != 1 {
			db.cache.Add(latestElement)
			return true, latestElement.Value
		}
	}
	return false, nil
}

func (db *DataBase) Delete(key string) bool{

	found, _  := db.Get(key)
	if !found {
		return false
	}

	db.cache.Delete(key)

	element := Element.InitElement(key, []byte("0"), 1)
	elements := db.lsm.Memtable.Add(*element)
	if elements != nil {
		level := 0
		index := LSMTree.GetLastIndex(0)

		newLevel := strconv.Itoa(level)
		newIndex := strconv.Itoa(index)
		DataFilePath := "data/data/data_" + newLevel + "_" + newIndex + ".bin"
		IndexFilePath := "data/index/index_" + newLevel + "_" + newIndex + ".bin"
		SummeryFilePath := "data/summery/summery_" + newLevel + "_" + newIndex + ".bin"
		FilterFilePath := "data/filter/filter_" + newLevel + "_" + newIndex + ".bin"
		MetadataFilePath := "data/metadata/metadata_" + newLevel + "_" + newIndex + ".bin"
		TOCFilePath := "data/TOC/toc_" + newLevel + "_" + newIndex + ".bin"

		sstable := SSTable.InitSSTable(elements, DataFilePath, IndexFilePath, SummeryFilePath, FilterFilePath, MetadataFilePath,
			TOCFilePath)

		db.lsm.Add(*sstable)
	}
	return true
}

