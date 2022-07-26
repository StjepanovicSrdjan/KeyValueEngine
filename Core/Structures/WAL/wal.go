package WAL

import (
	"KeyValueEngine/Core/Structures/Element"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"hash/crc32"
	"os"
	"strconv"
	"strings"
)

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

type WAL struct {
	maxSize uint8
	currentSize uint8
	filePath string
	deleteSize uint8
}

func InitWAL (size uint8, deleteS uint8) *WAL {
	wal := WAL{}
	wal.maxSize = size
	wal.deleteSize = deleteS

	allFiles, _ := os.ReadDir("data/segments")
	lastIndex := 0
	for _, file := range (allFiles){
		fname := file.Name()
		s := strings.Split(fname, ".bin")
		s = strings.Split(s[0], "wal")
		n,_ := strconv.Atoi(s[1])
		if n > lastIndex{
			lastIndex = n
		}
	}

	if lastIndex > 0 {
		//wal.file,_ = os.OpenFile("Segments/wal" + strconv.Itoa(lastIndex) + ".bin", os.O_RDWR, 0777)
		wal.filePath = "data/segments/wal" + strconv.Itoa(lastIndex) + ".bin"
	}else{
		file,_ := os.Create("data/segments/wal1.bin")
		wal.filePath = file.Name()
		//wal.filePath = "data/segments/wal1.bin"
	}

	return &wal
}

func (wal *WAL) Add (key string, value []byte) bool{

	elem := Element.InitElement(key, value, 0)
	line := elem.Encode()

	file, err := os.OpenFile(wal.filePath, os.O_RDWR, 0777)
	if err != nil{panic(err)}
	defer file.Close()
	writeLine(file, line)

	wal.currentSize += 1
	if wal.currentSize >= wal.maxSize{
		fname := wal.filePath
		s := strings.Split(fname, ".bin")
		s = strings.Split(s[0], "wal")
		n, err := strconv.Atoi(s[1])
		if err != nil {
			return false
		}

/*		err = wal.file.Close()
		if err != nil{
			return false
		}*/

		_, err = os.Create("data/segments/wal" + strconv.Itoa(n+1) + ".bin")
		if err != nil{
			return false
		}
		wal.filePath = "data/segments/wal" + strconv.Itoa(n+1) + ".bin"

		segments, err := os.ReadDir("data/segments")
		if err != nil{
			return false
		}
		if len(segments) > int(wal.deleteSize) {
			wal.ReduceSegments()
		}
	}
	return true
}

func (wal *WAL) Delete (key string, value []byte){

	elem := Element.InitElement(key, value, 1)
	line := elem.Encode()

	file, err := os.OpenFile(wal.filePath, os.O_RDWR, 0777)
	if err != nil{panic(err)}
	defer file.Close()
	writeLine(file, line)

	wal.currentSize += 1
	if wal.currentSize >= wal.maxSize{
		fname := wal.filePath
		s := strings.Split(fname, ".bin")
		s = strings.Split(s[0], "wal")
		n,_ := strconv.Atoi(s[1])
		//_ = wal.file.Close()
		_, _ = os.Create("data/segments/wal" + strconv.Itoa(n+1) + ".bin")
		wal.filePath = "data/segments/wal" + strconv.Itoa(n+1) + ".bin"
		segments, _ := os.ReadDir("data/segments")
		if len(segments) > int(wal.deleteSize) {
			wal.ReduceSegments()
		}
	}
}

func writeLine(file *os.File, line []byte) {
	stat, _ := file.Stat()
	fmt.Println(stat)
	fSize := stat.Size()
	_ = file.Truncate(fSize + int64(len(line)))

	mmapFile,_ := mmap.Map(file,  mmap.RDWR, 0)
	copy(mmapFile[fSize:], line)
	_ = mmapFile.Flush()
	_ = mmapFile.Unmap()
}

func (wal *WAL) ReduceSegments() {
	for i := 1; uint8(i) <= wal.deleteSize; i++ {
		_ = os.Remove("data/segments/wal" + strconv.Itoa(i) + ".bin")
	}
	allFiles, _ := os.ReadDir("data/segments")
	for index, file := range allFiles{
		str := file.Name()
		_ = os.Rename("data/segments/"+str, "data/segments/wal"+strconv.Itoa(index+1)+".bin")
	}
}
