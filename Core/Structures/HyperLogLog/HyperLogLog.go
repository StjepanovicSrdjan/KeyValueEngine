package HyperLogLog

import(
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
	m   uint64
	p   uint8
	reg []uint8
	hash hash.Hash32
}

func InitHLL(p uint8) (*HLL) {
	if p > HLL_MAX_PRECISION || p < HLL_MIN_PRECISION {
		panic("Incorrect p.")
	}
	M := uint64(math.Pow(2, float64(p)))
	return &HLL{
		m: M,
		p: p,
		reg: make([]uint8, M, M),
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
	bucketNum := b >> (32 - hll.p)
	trailingZeros := bits.TrailingZeros32(b)
	if hll.reg[bucketNum] < uint8(trailingZeros) {
		hll.reg[bucketNum] = uint8(trailingZeros)
	}
}

func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.reg {
		sum += math.Pow(math.Pow(2.0, float64(val)),-1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}
