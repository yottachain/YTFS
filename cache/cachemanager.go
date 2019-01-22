package cache

import (
	"fmt"
	// "sync"

	lru "github.com/hashicorp/golang-lru"

	"github.com/yotta-disk/errors"
)

type CacheManager struct {
	// Size
	caps		uint64

	// Implementation
	*lru.Cache

	// Explicit locker for those complex operations
	// sync.Mutex

	// statistics
	itemSize	uint64
	hit			uint64
	access		uint64
}

const logCache = false

func NewCacheManager(itemSize uint32, cacheCaps uint64, onEvicted func(key interface{}, value interface{})) (*CacheManager, error) {
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

	return &CacheManager{
		lruCaps,
		cache,
		// sync.Mutex{},
		(uint64)(itemSize),
		0,
		0,
	}, nil
}

func (cm *CacheManager) Add(key, value interface{}) bool {
	if logCache {
		fmt.Printf("Cache Add <%v:%v>\n", key, value)
	}
	return cm.Cache.Add(key, value)
}

func (cm *CacheManager) Contains(key interface{}) bool {
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

func (cm *CacheManager) String() string {
	return fmt.Sprintf( "Cache slot number: %d\n"  +
						"Occupied cache slot: %d\n" +
						"Cache slot item size: %d\n" +
						"Cache hit: %d\n" +
						"Cache miss: %d\n", cm.caps, cm.Len(), cm.itemSize, cm.hit, cm.access - cm.hit)
}

