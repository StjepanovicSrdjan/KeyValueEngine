package LSMTree

import (
	"KeyValueEngine/Core/Structures/Element"
	"KeyValueEngine/Core/Structures/Memtable"
	"KeyValueEngine/Core/Structures/SSTable"
	"io/ioutil"
	"strconv"
)

type LSM struct {
	memtable Memtable.Memtable
	ssTables [][]SSTable.SSTable
	maxLevel uint16
	maxTablesInLevel uint16
}

func InitLSM (memtable Memtable.Memtable, maxLevel, maxTablesInLevel uint16) (*LSM){
	return &LSM{
		memtable: memtable,
		ssTables: make([][]SSTable.SSTable, maxLevel),
		maxLevel: maxLevel,
		maxTablesInLevel: maxTablesInLevel,
	}
}

func (lsm *LSM) UploadData() {
	files, _ := ioutil.ReadDir("data/data")

	if len(files) == 0 {
		return
	}

	tablesNumInLevel := make([]int, lsm.maxLevel)

	for _, file := range files {
		currentName := file.Name()

		level, index := getLevelAndIndex(currentName)

		if tablesNumInLevel[level] < index {
			tablesNumInLevel[level] = index
		}
	}
	numOfLevels := int(lsm.maxLevel)
	for i := 0; i < numOfLevels; i++ {
		if tablesNumInLevel[i] != 0 {
			lsm.ssTables[i] = make([]SSTable.SSTable, tablesNumInLevel[i])
		}else{
			lsm.ssTables[i] = make([]SSTable.SSTable, 0)
		}
	}

	for _, file := range files{
		level, index := getLevelAndIndex(file.Name())
		levelStr := strconv.Itoa(level)
		indexStr := strconv.Itoa(index)
		ssTable := SSTable.SSTable{
			DataFilePath: "data/data/data_" + levelStr + "_" + indexStr + ".bin",
			IndexFilePath: "data/index/index_" + levelStr + "_" + indexStr + ".bin",
			SummeryFilePath: "data/summery/summery_" + levelStr + "_" + indexStr + ".bin",
			FilterFilePath: "data/filer/filter_" + levelStr + "_" + indexStr + ".bin",
			MetadataFilePath: "data/metadata/metadata_" + levelStr + "_" + indexStr + ".bin",
			TOCFilePath: "data/TOC/toc_" + levelStr + "_" + indexStr + ".bin",
		}
		lsm.ssTables[level][index] = ssTable
	}
}

func (lsm *LSM) Add(ssTable SSTable.SSTable) {
	lsm.ssTables[0] = append(lsm.ssTables[0], ssTable)
	for i := 0; i < int(lsm.maxLevel); i++ {
		if len(lsm.ssTables[i]) < int(lsm.maxTablesInLevel){
			break
		}

		newSStable := lsm.MergeLevel(i)
		lsm.ssTables[i] = append(lsm.ssTables[i], newSStable)
		for j := 0; j < len(lsm.ssTables[i]); j++ {
			lsm.ssTables[i][j].Delete()
		}
	}

}

func (lsm *LSM) MergeLevel(level int) (SSTable.SSTable) {
	firstTable := lsm.ssTables[level][0]
	newIndex := getLastIndex(level + 1)
	for i := 1; i < int(lsm.maxTablesInLevel); i++ {
		secondTable := lsm.ssTables[level][i]
		newTable := lsm.MergeSSTables(firstTable, secondTable, level + 1, newIndex)

		firstTable = newTable
	}

	return firstTable
}


func (lsm *LSM) MergeSSTables(firstTable, secondTable SSTable.SSTable, level, index int) (SSTable.SSTable){
	firstData := SSTable.ReadAll(firstTable.DataFilePath)
	secondData := SSTable.ReadAll(secondTable.DataFilePath)
	firstIndex := 0
	secondIndex := 0

	newData := make([]Element.Element, 0)

	for{
		if firstData[firstIndex].Key == secondData[secondIndex].Key{
			if firstData[firstIndex].Timestamp > secondData[secondIndex].Timestamp {
				if firstData[firstIndex].Tombstone == 0 {
					newData = append(newData, firstData[firstIndex])
				}
			}else{
				if secondData[secondIndex].Tombstone == 0{
					newData = append(newData, secondData[secondIndex])
				}
			}
			firstIndex++
			secondIndex++
			if firstIndex == len(firstData) {
				for i := secondIndex; i < len(secondData); i++ {
					newData = append(newData, secondData[i])
				}
				break
			}
			if secondIndex == len(secondData) {
				for i := firstIndex; i < len(firstData); i++ {
					newData = append(newData, firstData[i])
				}
				break
			}
		}else{
			if firstData[firstIndex].Key < secondData[secondIndex].Key {
				if firstData[firstIndex].Tombstone == 0 {
					newData = append(newData, firstData[firstIndex])
				}
				firstIndex++
				if firstIndex == len(firstData){
					for i := secondIndex; i < len(secondData); i++ {
						newData = append(newData, secondData[i])
					}
					break
				}
			}else{
				if secondData[secondIndex].Tombstone == 0{
					newData = append(newData, secondData[secondIndex])
				}
				secondIndex++
				if secondIndex == len(secondData){
					for i := firstIndex; i < len(firstData); i++ {
						newData = append(newData, firstData[i])
					}
					break
				}
			}
		}
	}

	newLevel := strconv.Itoa(level)
	newIndex := strconv.Itoa(index)
	DataFilePath := "data/data/data_" + newLevel + "_" + newIndex + ".bin"
	IndexFilePath := "data/index/index_" + newLevel + "_" + newIndex + ".bin"
	SummeryFilePath := "data/summery/summery_" + newLevel + "_" + newIndex + ".bin"
	FilterFilePath := "data/filer/filter_" + newLevel + "_" + newIndex + ".bin"
	MetadataFilePath := "data/metadata/metadata_" + newLevel + "_" + newIndex + ".bin"
	TOCFilePath := "data/TOC/toc_" + newLevel + "_" + newIndex + ".bin"

	return *SSTable.InitSSTable(newData, DataFilePath, IndexFilePath, SummeryFilePath, FilterFilePath, MetadataFilePath,
		TOCFilePath)
}
