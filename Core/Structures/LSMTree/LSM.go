package LSMTree

import (
	"KeyValueEngine/Core/Structures/Element"
	"KeyValueEngine/Core/Structures/Memtable"
	"KeyValueEngine/Core/Structures/SSTable"
	"io/ioutil"
	"strconv"
)

type LSM struct {
	Memtable Memtable.Memtable
	SSTables [][]SSTable.SSTable
	MaxLevel         uint16
	MaxTablesInLevel uint16
}

func InitLSM (memtable Memtable.Memtable, maxLevel, maxTablesInLevel uint16) (*LSM){
	return &LSM{
		Memtable:         memtable,
		SSTables:         make([][]SSTable.SSTable, maxLevel),
		MaxLevel:         maxLevel,
		MaxTablesInLevel: maxTablesInLevel,
	}
}

func (lsm *LSM) UploadData() {
	files, _ := ioutil.ReadDir("data/data")

	if len(files) == 0 {
		return
	}

	tablesNumInLevel := make([]int, lsm.MaxLevel)

	for _, file := range files {
		currentName := file.Name()

		level, index := GetLevelAndIndex(currentName)

		if tablesNumInLevel[level] < index {
			tablesNumInLevel[level] = index
		}
	}
	numOfLevels := int(lsm.MaxLevel)
	for i := 0; i < numOfLevels; i++ {
		if tablesNumInLevel[i] != 0 {
			lsm.SSTables[i] = make([]SSTable.SSTable, tablesNumInLevel[i])
		}else{
			lsm.SSTables[i] = make([]SSTable.SSTable, 0)
		}
	}

	for _, file := range files{
		level, index := GetLevelAndIndex(file.Name())
		levelStr := strconv.Itoa(level)
		indexStr := strconv.Itoa(index)
		ssTable := SSTable.SSTable{
			DataFilePath: "data/data/data_" + levelStr + "_" + indexStr + ".bin",
			IndexFilePath: "data/index/index_" + levelStr + "_" + indexStr + ".bin",
			SummeryFilePath: "data/summery/summery_" + levelStr + "_" + indexStr + ".bin",
			FilterFilePath: "data/filter/filter_" + levelStr + "_" + indexStr + ".bin",
			MetadataFilePath: "data/metadata/metadata_" + levelStr + "_" + indexStr + ".bin",
			TOCFilePath: "data/TOC/toc_" + levelStr + "_" + indexStr + ".bin",
		}
		lsm.SSTables[level][index] = ssTable
	}
}

func (lsm *LSM) Add(ssTable SSTable.SSTable) {
	lsm.SSTables[0] = append(lsm.SSTables[0], ssTable)
	for i := 0; i < int(lsm.MaxLevel); i++ {
		if len(lsm.SSTables[i]) < int(lsm.MaxTablesInLevel){
			break
		}

		newSStable := lsm.MergeLevel(i)
		lsm.SSTables[i] = append(lsm.SSTables[i], newSStable)
		for j := 0; j < len(lsm.SSTables[i]); j++ {
			lsm.SSTables[i][j].Delete()
		}
	}

}

func (lsm *LSM) MergeLevel(level int) (SSTable.SSTable) {
	firstTable := lsm.SSTables[level][0]
	newIndex := GetLastIndex(level + 1)
	for i := 1; i < int(lsm.MaxTablesInLevel); i++ {
		secondTable := lsm.SSTables[level][i]
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
	FilterFilePath := "data/filter/filter_" + newLevel + "_" + newIndex + ".bin"
	MetadataFilePath := "data/metadata/metadata_" + newLevel + "_" + newIndex + ".bin"
	TOCFilePath := "data/TOC/toc_" + newLevel + "_" + newIndex + ".bin"

	return *SSTable.InitSSTable(newData, DataFilePath, IndexFilePath, SummeryFilePath, FilterFilePath, MetadataFilePath,
		TOCFilePath)
}
