package BloomFilter

import (
	"encoding/gob"
	"hash"
	"os"
)

type BloomFilter struct{
	Bitfield []bool
	N uint
	K            uint
	M            uint
	hashFunction []hash.Hash32
}

func InitBF(expectedEl int, fpRate float64)  *BloomFilter {
	size := CalculateM(expectedEl, fpRate)
	hashNum := CalculateK(expectedEl, size)
	return &BloomFilter{
		Bitfield:     make([]bool, size),
		M:            size,
		K:            hashNum,
		hashFunction: CreateHashFunctions(hashNum),
	}
}

func (bf *BloomFilter) Add(item string) {
	arr := []byte(item)
	//hashValues := bf.Hash(arr)

	i := uint(0)

	for{
		if i >= bf.K {
			break
		}
		bf.hashFunction[i].Reset()
		if _, err := bf.hashFunction[i].Write(arr); err != nil{
			panic(err)
		}
		index := bf.hashFunction[i].Sum32() % uint32(bf.M)
		bf.Bitfield[uint(index)] = true

		i += 1
	}

	bf.N += 1

}

func (bf *BloomFilter) Contains(item string) bool{
	arr := []byte(item)
	//hashValues := bf.Hash(arr)

	i := uint(0)

	for{
		if i >= bf.K {
			break
		}
		bf.hashFunction[i].Reset()
		if _, err := bf.hashFunction[i].Write(arr); err != nil{
			panic(err)
		}
		index := bf.hashFunction[i].Sum32() % uint32(bf.M)
		if bf.Bitfield[uint(index)] == false{
			return false
		}
		i += 1
	}
	return true
}


func (bf *BloomFilter) Hash(item []byte) []uint32{
	var result []uint32

	for _, HashFunc := range bf.hashFunction {
		_, err := HashFunc.Write(item)
		if err != nil {
			return nil
		}
		result = append(result, HashFunc.Sum32())
		HashFunc.Reset()
	}

	return result

}

func (bf *BloomFilter) Encode(filterFilePath string) {
	file, err := os.Create(filterFilePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	encoder := gob.NewEncoder(file)
	if err = encoder.Encode(&bf); err != nil {
		panic(err)
	}
}

func (bf *BloomFilter) Decode(filterFilePath string) {
	file, err := os.OpenFile(filterFilePath, os.O_RDWR, 0777)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	decoder := gob.NewDecoder(file)
	if err = decoder.Decode(&bf); err != nil{
		panic(err)
	}
	bf.hashFunction = CreateHashFunctions(bf.K)
}

