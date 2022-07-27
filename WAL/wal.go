package WAL

import (
	"KeyValueEngine/Element"
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
	file *os.File
	deleteSize uint8
}

func InitWAL (size uint8, deleteS uint8) *WAL {
	wal := WAL{}
	wal.maxSize = size
	wal.deleteSize = deleteS

	allFiles, _ := os.ReadDir("Segments")
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
		wal.file,_ = os.OpenFile("Segments/wal" + strconv.Itoa(lastIndex) + ".bin", os.O_RDWR, 0777)
	}else{
		wal.file,_ = os.Create("Segments/wal1.bin")

	}

	return &wal
}

func (wal *WAL) Add (key string, value []byte){

	elem := Element.NewElement(key, value, 0)
	line := elem.Encode()

	writeLine(wal.file, line)

	wal.currentSize += 1
	if wal.currentSize >= wal.maxSize{
		fname := wal.file.Name()
		s := strings.Split(fname, ".bin")
		s = strings.Split(s[0], "wal")
		n,_ := strconv.Atoi(s[1])
		_ = wal.file.Close()
		wal.file, _ = os.Create("Segments/wal" + strconv.Itoa(n+1) + ".bin")
		segments, _ := os.ReadDir("Segments")
		if len(segments) > int(wal.deleteSize) {
			wal.ReduceSegments()
		}
	}
}

func (wal *WAL) Delete (key string, value []byte){

	elem := Element.NewElement(key, value, 1)
	line := elem.Encode()

	writeLine(wal.file, line)

	wal.currentSize += 1
	if wal.currentSize >= wal.maxSize{
		fname := wal.file.Name()
		s := strings.Split(fname, ".bin")
		s = strings.Split(s[0], "wal")
		n,_ := strconv.Atoi(s[1])
		_ = wal.file.Close()
		wal.file, _ = os.Create("Segments/wal" + strconv.Itoa(n+1) + ".bin")
		segments, _ := os.ReadDir("Segments")
		if len(segments) > int(wal.deleteSize) {
			wal.ReduceSegments()
		}
	}
}

func writeLine(file *os.File, line []byte) {
	stat, _ := file.Stat()
	fSize := stat.Size()
	_ = file.Truncate(fSize + int64(len(line)))

	mmapFile,_ := mmap.Map(file,  mmap.RDWR, 0)
	copy(mmapFile[fSize:], line)
	_ = mmapFile.Flush()
	_ = mmapFile.Unmap()
}

func (wal *WAL) ReduceSegments() {
	for i := 1; uint8(i) <= wal.deleteSize; i++ {
		_ = os.Remove("Segments/wal" + strconv.Itoa(i) + ".bin")
	}
	allFiles, _ := os.ReadDir("Segments")
	for index, file := range allFiles{
		str := file.Name()
		_ = os.Rename("Segments/"+str, "Segments/wal"+strconv.Itoa(index+1)+".bin")
	}
}
