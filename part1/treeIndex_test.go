package part1

import (
	"testing"
)

func BenchmarkTreeIndexSearch_1000(b *testing.B) {
	idx := createIndex("tree", 1000)
	min, max := searchBorders()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.Search(min, max, nil)
	}
}

func BenchmarkTreeIndexSearch_10000(b *testing.B) {
	idx := createIndex("tree", 10000)
	min, max := searchBorders()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.Search(min, max, nil)
	}
}

func BenchmarkTreeIndexSearch_100000(b *testing.B) {
	idx := createIndex("tree", 100000)
	min, max := searchBorders()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.Search(min, max, nil)
	}
}

func BenchmarkTreeIndexSearch_1000000(b *testing.B) {
	idx := createIndex("tree", 1000000)
	min, max := searchBorders()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.Search(min, max, nil)
	}
}
