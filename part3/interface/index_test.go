package iface

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/visheratin/tsdb-challenges/part3"
)

var dataPath = "./data-binary"

func prepare(b *testing.B) {
	err := os.MkdirAll(dataPath, 0777)
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkInterfaceStoreCreate_small(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewIndex("123", 3600000, "interface", dataPath, testDataSmall, part3.Float64)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInterfaceStoreExtract_small(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	idx, err := NewIndex("123", 3600000, "interface", dataPath, testDataSmall, part3.Float64)
	if err != nil {
		b.Fatal(err)
	}
	finishTimestamp := testDataSmall[len(testDataSmall)-1].Timestamp()
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

func BenchmarkInterfaceStoreCreate_medium(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewIndex("123", 3600000, "interface", dataPath, testDataMedium, part3.Float64)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInterfaceStoreExtract_medium(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	idx, err := NewIndex("123", 3600000, "interface", dataPath, testDataMedium, part3.Float64)
	if err != nil {
		b.Fatal(err)
	}
	finishTimestamp := testDataMedium[len(testDataMedium)-1].Timestamp()
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

func BenchmarkInterfaceStoreCreate_large(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewIndex("123", 3600000, "interface", dataPath, testDataLarge, part3.Float64)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInterfaceStoreExtract_large(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	idx, err := NewIndex("123", 3600000, "interface", dataPath, testDataLarge, part3.Float64)
	if err != nil {
		b.Fatal(err)
	}
	finishTimestamp := testDataLarge[len(testDataLarge)-1].Timestamp()
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
