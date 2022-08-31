package HyperLogLog

import(
	"bytes"
	"encoding/gob"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"math/bits"
	"time"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

type HLL struct {
	M   uint64
	P    uint8
	Reg  []uint8
	hash hash.Hash32
}

func InitHLL(p uint8) (*HLL) {
	if p > HLL_MAX_PRECISION || p < HLL_MIN_PRECISION {
		panic("Incorrect P.")
	}
	M := uint64(math.Pow(2, float64(p)))
	return &HLL{
		M:    M,
		P:    p,
		Reg:  make([]uint8, M, M),
		hash: murmur3.New32WithSeed(uint32(time.Now().Unix())),
	}
}

func (hll *HLL) Add(item string) {
	hll.hash.Reset()
	_, err := hll.hash.Write([]byte(item))
	if err != nil {
		panic(err)
	}
	b := hll.hash.Sum32()
	bucketNum := b >> (32 - hll.P)
	trailingZeros := bits.TrailingZeros32(b)
	if hll.Reg[bucketNum] < uint8(trailingZeros) {
		hll.Reg[bucketNum] = uint8(trailingZeros)
	}
}

func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.Reg {
		sum += math.Pow(math.Pow(2.0, float64(val)),-1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.M))
	estimation := alpha * math.Pow(float64(hll.M), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.M) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.M) * math.Log(float64(hll.M)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.Reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func (hll *HLL) Encode() []byte {
	encoded := bytes.Buffer{}
	encoder := gob.NewEncoder(&encoded)
	err := encoder.Encode(hll)
	if err != nil {
		panic(err.Error())
	}
	return encoded.Bytes()
}

func (hll *HLL) Decode(data []byte) {
	encoded := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(encoded)
	err := decoder.Decode(hll)
	if err != nil {
		panic(err.Error())
	}
	hll.hash = murmur3.New32WithSeed(uint32(time.Now().Unix()))
}
