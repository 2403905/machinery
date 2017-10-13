package common

import "sync/atomic"

//AtomicBool implements a synchronized boolean value
type AtomicBool struct {
	val int32
}

// NewAtomicBool generates a new AtomicBoolean instance.
func NewAtomicBool(value bool) *AtomicBool {
	var i int32
	if value {
		i = 1
	}
	return &AtomicBool{
		val: i,
	}
}

// Get atomically retrieves the boolean value.
func (ab *AtomicBool) Get() bool {
	return atomic.LoadInt32(&(ab.val)) != 0
}

// Set atomically sets the boolean value.
func (ab *AtomicBool) Set(newVal bool) {
	var i int32
	if newVal {
		i = 1
	}
	atomic.StoreInt32(&(ab.val), int32(i))
}
