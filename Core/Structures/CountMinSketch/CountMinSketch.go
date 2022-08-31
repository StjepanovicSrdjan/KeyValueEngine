package CountMinSketch

import (
	"bytes"
	"encoding/gob"
	"hash"
)

type CountMinSketch struct {
	K         uint
	M         uint
	hashFuncs []hash.Hash32
	Data      [][]uint
}

func InitCMS(epsilon, delta float64) *CountMinSketch {
	hashNum := CalculateK(delta)
	rowNum := CalculateM(epsilon)
	ret := CountMinSketch{
		K:         hashNum,
		M:         rowNum,
		hashFuncs: CreateHashFunctions(hashNum),
	}
	ret.Data = make([][] uint, ret.K)
	for i := range ret.Data {
		ret.Data[i] = make([]uint, ret.M)
	}
	return &ret
}

func (cms *CountMinSketch) Add(item string) {

	for i := 0; i < int(cms.K); i++ {
		cms.hashFuncs[i].Reset()
		_, err := cms.hashFuncs[i].Write([]byte(item))
		if err != nil {
			return
		}
		j := cms.hashFuncs[i].Sum32() % uint32(cms.M)
		cms.Data[i][j] += 1
	}
}

func (cms *CountMinSketch) GetFrequency(item string) uint {
	fs := make([]uint, cms.K, cms.K)
	for i := 0; i < int(cms.K); i++ {

		cms.hashFuncs[i].Reset()
		_, err := cms.hashFuncs[i].Write([]byte(item))
		if err != nil {
			return 0
		}
		j := cms.hashFuncs[i].Sum32() % uint32(cms.M)
		fs[i] = cms.Data[i][j]
	}
	min := fs[0]
	for i := 1; i < int(cms.K); i++ {
		if fs[i] < min {
			min = fs[i]
		}
	}
	return min
}

func (cms *CountMinSketch) Encode() []byte {
	/*file, err := os.Create(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	encoder := gob.NewEncoder(file)
	if err = encoder.Encode(&c); err != nil {
		panic(err)
	}*/
	encoded := bytes.Buffer{}
	encoder := gob.NewEncoder(&encoded)
	err := encoder.Encode(cms)
	if err != nil {
		panic(err.Error())
	}
	return encoded.Bytes()
}

func (cms *CountMinSketch) Decode(data []byte) {
	/*decoder := gob.NewDecoder(Data)
	var c CountMinSketch
	err = decoder.Decode(&c)
	c.hashFuncs = CreateHashFunctions(c.K)
	file.Close()
	return &c*/

	encoded := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(encoded)
	err := decoder.Decode(cms)
	if err != nil {
		panic(err.Error())
	}
	cms.hashFuncs = CreateHashFunctions(cms.K)
}
