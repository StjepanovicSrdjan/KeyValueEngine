package TokenBucket

import (
	"time"
)

type TokenBucket struct {
	maxTokens uint16
	remainingTokens uint16
	lastRefill time.Time
	resetInterval time.Duration
}

func InitTokenBucket(_maxTokens uint16, _resetInterval time.Duration) *TokenBucket {
	if _resetInterval <= 0 {
		panic("Nepravilan reset interval")
	}

	return &TokenBucket{
		maxTokens: _maxTokens,
		remainingTokens: _maxTokens,
		lastRefill: time.Now(),
		resetInterval: _resetInterval,
	}
}

func (tb *TokenBucket) Refill() {
	tb.remainingTokens = tb.maxTokens
	tb.lastRefill = time.Now()
}

func (tb *TokenBucket) HasTokens() bool {
	now := time.Now()
	if now.Sub(tb.lastRefill) > tb.resetInterval {
		tb.Refill()
	}
	if tb.remainingTokens <= 0 {
		return false
	}
	tb.remainingTokens--
	return true
}

