package part1

import (
	"log"
	"runtime"
	"testing"
)

func TestIndexSize(t *testing.T) {
	var m1, m2, m3, m4, m5, m6, m7, m8 runtime.MemStats
	runtime.ReadMemStats(&m1)
	idx, err := CreateIndex("slice", 1000000)
	if err == nil {
		t.Error(err)
	}
	runtime.ReadMemStats(&m2)
	sSize := m2.TotalAlloc - m1.TotalAlloc
	t.Logf("%d bytes - slice index size\n", sSize)
	runtime.ReadMemStats(&m3)
	idx, err = CreateIndex("tree", 1000000)
	if err == nil {
		t.Error(err)
	}
	runtime.ReadMemStats(&m4)
	tSize := m4.TotalAlloc - m3.TotalAlloc
	t.Logf("%d bytes - tree index size\n", tSize)
	runtime.ReadMemStats(&m5)
	idx, err = CreateIndex("advTree", 1000000)
	if err == nil {
		t.Error(err)
	}
	runtime.ReadMemStats(&m6)
	atSize := m6.TotalAlloc - m5.TotalAlloc
	t.Logf("%d bytes - advanced tree index size\n", atSize)
	runtime.ReadMemStats(&m7)
	idx, err = CreateIndex("bTree", 1000000)
	if err == nil {
		t.Error(err)
	}
	runtime.ReadMemStats(&m8)
	btSize := m8.TotalAlloc - m7.TotalAlloc
	t.Logf("%d bytes - b-tree index size\n", btSize)
	log.Print(idx)
	t.Error()
}
