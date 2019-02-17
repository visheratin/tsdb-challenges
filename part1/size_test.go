package part1

import (
	"testing"
)

func TestIndexSize(t *testing.T) {
	idx := createIndex("slice", 1000000)
	sSize := idx.Size()
	t.Logf("%d bytes - slice index size\n", sSize)
	idx = createIndex("tree", 1000000)
	tSize := idx.Size()
	t.Logf("%d bytes - tree index size\n", tSize)
	idx = createIndex("advTree", 1000000)
	atSize := idx.Size()
	t.Logf("%d bytes - advanced tree index size\n", atSize)
	idx = createIndex("bTree", 1000000)
	btSize := idx.Size()
	t.Logf("%d bytes - b-tree index size\n", btSize)
	t.Error()
}
