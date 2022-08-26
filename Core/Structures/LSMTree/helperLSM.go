package LSMTree

import (
	"io/ioutil"
	"strconv"
	"strings"
)

func getLevelAndIndex (fileName string) (int, int) {
	tokens := strings.Split(fileName, "_")
	level, err := strconv.Atoi(tokens[1])
	if err != nil {
		panic(err)
	}
	index, err := strconv.Atoi(tokens[2])
	if err != nil {
		panic(err)
	}
	return level, index
}

func getLastIndex(level int) (int){
	currentHighest := 0
	files, _ := ioutil.ReadDir("data/data")

	for _, file := range files {
		tokens := strings.Split(file.Name(), "_")
		currentIndex, _ := strconv.Atoi(tokens[2])
		currentLevel, _ := strconv.Atoi(tokens[1])
		if currentLevel == level {
			if currentHighest < currentIndex {
				currentHighest = currentIndex
			}
		}
	}
	return currentHighest
}
