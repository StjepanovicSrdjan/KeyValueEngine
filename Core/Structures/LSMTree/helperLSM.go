package LSMTree

import (
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

