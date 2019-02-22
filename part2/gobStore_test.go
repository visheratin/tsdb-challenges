package part2

import (
	"math/rand"
	"testing"
	"time"
)

func BenchmarkGobStoreCreate_small(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := NewIndex("123", 3600000, "gob", "./data-gob", testDataSmall)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGobStoreExtract_small(b *testing.B) {
	idx, err := NewIndex("123", 3600000, "gob", "./data-gob", testDataSmall)
	if err != nil {
		b.Fatal(err)
	}
	finishTimestamp := testDataSmall[len(testDataSmall)-1].Timestamp
	minStart := finishTimestamp - int64(500000000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		r := rand.New(rand.NewSource(time.Now().Unix()))
		s := r.Int63n(minStart)
		f := s + 500000000
		b.StartTimer()
		_, err := idx.Extract(s, f)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGobStoreCreate_medium(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := NewIndex("123", 3600000, "gob", "./data-gob", testDataMedium)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGobStoreExtract_medium(b *testing.B) {
	idx, err := NewIndex("123", 3600000, "gob", "./data-gob", testDataMedium)
	if err != nil {
		b.Fatal(err)
	}
	finishTimestamp := testDataMedium[len(testDataMedium)-1].Timestamp
	minStart := finishTimestamp - int64(500000000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		r := rand.New(rand.NewSource(time.Now().Unix()))
		s := r.Int63n(minStart)
		f := s + 500000000
		b.StartTimer()
		_, err := idx.Extract(s, f)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGobStoreCreate_large(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := NewIndex("123", 3600000, "gob", "./data-gob", testDataLarge)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGobStoreExtract_large(b *testing.B) {
	idx, err := NewIndex("123", 3600000, "gob", "./data-gob", testDataLarge)
	if err != nil {
		b.Fatal(err)
	}
	finishTimestamp := testDataLarge[len(testDataLarge)-1].Timestamp
	minStart := finishTimestamp - int64(500000000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		r := rand.New(rand.NewSource(time.Now().Unix()))
		s := r.Int63n(minStart)
		f := s + 500000000
		b.StartTimer()
		_, err := idx.Extract(s, f)
		if err != nil {
			b.Fatal(err)
		}
	}
}
