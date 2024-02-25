package util

import (
	"bytes"
	"encoding/binary"
	"fmt"
)
import "math"

func BytesPlusPlusRecursionFun(data []byte) bool {
	var byteMax = []byte{math.MaxUint8}
	if len(data) == 0 {
		return false
	}
	for i := len(data) - 1; i >= 0; i-- {
		if !bytes.Equal(data[i:i+1], byteMax) {
			data[i]++
			break
		}
		if BytesPlusPlusRecursionFun(data[0:i]) {
			data[i] = 0
			break
		}
	}

	return true
}

func Bytes32XorUint32(bytes32 []byte, minerId uint32) ([]byte, error) {
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, minerId)
	minerId32Byte := make([]byte, 32)
	copy(minerId32Byte, bytes)
	if len(minerId32Byte) != len(bytes32) {
		return nil, fmt.Errorf("challenge value length not equals 32 ")
	}

	realValue := make([]byte, len(minerId32Byte))

	for i := 0; i < len(minerId32Byte); i++ {
		realValue[i] = minerId32Byte[i] ^ bytes32[i]
	}

	return realValue, nil
}
