package part2

import (
	"math/rand"
	"testing"
	"time"
)

var testDataSmall = generateData(300000)
var testDataMedium = generateData(3000000)
var testDataLarge = generateData(30000000)

func BenchmarkGobStoreCreate_small(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = NewIndex("123", 3600000, "gob", "./tmp", testDataSmall)
	}
}

func BenchmarkGobStoreExtract_small(b *testing.B) {
	idx, _ := NewIndex("123", 3600000, "gob", "./tmp", testDataSmall)
	finishTimestamp := testDataSmall[len(testDataSmall)-1].Timestamp
	minStart := finishTimestamp - int64(500000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		r := rand.New(rand.NewSource(time.Now().Unix()))
		s := r.Int63n(minStart)
		f := s + 500000
		b.StartTimer()
		_, _ = idx.Extract(s, f)
	}
}

func BenchmarkGobStoreCreate_medium(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = NewIndex("123", 3600000, "gob", "./tmp", testDataMedium)
	}
}

func BenchmarkGobStoreCreate_large(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = NewIndex("123", 3600000, "gob", "./tmp", testDataLarge)
	}
}
