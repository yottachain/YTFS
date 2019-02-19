// Package cache provides cache support used throughout YottaDisk.
package cache

import (
	"fmt"
	// "sync"

	lru "github.com/hashicorp/golang-lru"

	"github.com/yottachain/YTFS/errors"
)

// Manager the entry point of LRU cache used by YottaDisk
type Manager struct {
	// Size
	caps uint64

	// Implementation
	*lru.Cache

	// Explicit locker for those complex operations
	// sync.Mutex

	// statistics
	itemSize uint64
	hit      uint64
	access   uint64
}

const logCache = false

// NewCacheManager creates a new CacheManager, reports ErrConfigCache
// if open failed.
func NewCacheManager(itemSize uint32, cacheCaps uint64, onEvicted func(key interface{}, value interface{})) (*Manager, error) {
	if cacheCaps < (uint64)(itemSize) {
		cacheCaps = (uint64)(itemSize)
	}

	//TODO: adjust caps depends on real memory usage rather than maxTableSize
	lruCaps := cacheCaps / (uint64)(itemSize)
	if lruCaps == 0 {
		return nil, errors.ErrConfigCache
	}

	evictWithLog := func(key, value interface{}) {
		if logCache {
			fmt.Printf("Cache: evict <%v:%v>\n", key, value)
		}

		onEvicted(key, value)
	}

	cache, err := lru.NewWithEvict((int)(lruCaps), evictWithLog)
	if err != nil {
		return nil, errors.ErrConfigCache
	}

	return &Manager{
		lruCaps,
		cache,
		// sync.Mutex{},
		(uint64)(itemSize),
		0,
		0,
	}, nil
}

// Add wraps the LRU cache Add() which adds a new cache item.
func (cm *Manager) Add(key, value interface{}) bool {
	if logCache {
		fmt.Printf("Cache Add <%v:%v>\n", key, value)
	}
	return cm.Cache.Add(key, value)
}

// Contains wraps the LRU cache Contains() which check if a cache item exists.
func (cm *Manager) Contains(key interface{}) bool {
	if logCache {
		fmt.Printf("Check if <%v> contains.\n", key)
	}

	cm.access++
	if cm.Cache.Contains(key) {
		cm.hit++
		return true
	}

	return false
}

func (cm *Manager) String() string {
	return fmt.Sprintf("Cache slot number: %d\n"+
		"Occupied cache slot: %d\n"+
		"Cache slot item size: %d\n"+
		"Cache hit: %d\n"+
		"Cache miss: %d\n", cm.caps, cm.Len(), cm.itemSize, cm.hit, cm.access-cm.hit)
}
