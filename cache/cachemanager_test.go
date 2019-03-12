package cache

import (
	"fmt"
	"testing"
)

func TestCacheManager(t *testing.T) {
	cm, err := NewCacheManager(32, 32*1024, func(key, value interface{}) {
		fmt.Println("Remove", key, value)
	})
	if err != nil {
		t.Fatal(err)
	}

	table1 := map[uint32]uint32{}
	table1[1] = 1
	table1[2] = 2
	cm.Add(1, table1)

	table2 := map[uint32]uint32{}
	table2[1] = 3
	cm.Add(2, table2)

	if table, ok := cm.Get(1); ok {
		fmt.Println(table.(map[uint32]uint32)[1])
		fmt.Println(table.(map[uint32]uint32)[2])
	} else {
		t.Fail()
	}

	if table, ok := cm.Get(2); ok {
		fmt.Println(table.(map[uint32]uint32)[1])
		table.(map[uint32]uint32)[2] = 4
	} else {
		t.Fail()
	}

	if table, ok := cm.Get(2); ok {
		fmt.Println(table.(map[uint32]uint32)[2])
	} else {
		t.Fail()
	}

	cm.Purge()
}
