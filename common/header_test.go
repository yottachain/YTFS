package common

import (
	"fmt"
	"testing"
	"unsafe"
)

func TestHeaderPrint(t *testing.T) {
	header := Header{}

	infoMsg := ` 
		header layout info: size: %d, align: %d
		field          offset         size
		--------------+--------------+--------
		Tag            %03d           %d
		Version        %03d           %d
		YtfsCapability %03d           %d
		DataBlockSize  %03d           %d
		RangeCaps      %03d           %d
		RangeCoverage  %03d           %d
		HashOffset     %03d           %d
		DataCount      %03d           %d
		ResolveOffset  %03d           %d
		Reserved       %03d           %d
		`

	fmt.Printf(infoMsg,
		unsafe.Sizeof(header), unsafe.Alignof(header),
		unsafe.Offsetof(header.Tag), unsafe.Sizeof(header.Tag),
		unsafe.Offsetof(header.Version), unsafe.Sizeof(header.Version),
		unsafe.Offsetof(header.YtfsCapability), unsafe.Sizeof(header.YtfsCapability),
		unsafe.Offsetof(header.DataBlockSize), unsafe.Sizeof(header.DataBlockSize),
		unsafe.Offsetof(header.RangeCaps), unsafe.Sizeof(header.RangeCaps),
		unsafe.Offsetof(header.RangeCoverage), unsafe.Sizeof(header.RangeCoverage),
		unsafe.Offsetof(header.HashOffset), unsafe.Sizeof(header.HashOffset),
		unsafe.Offsetof(header.DataCount), unsafe.Sizeof(header.DataCount),
		unsafe.Offsetof(header.ResolveOffset), unsafe.Sizeof(header.ResolveOffset),
		unsafe.Offsetof(header.Reserved), unsafe.Sizeof(header.Reserved))
}
