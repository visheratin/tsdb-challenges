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

func BenchmarkInterfaceStoreCreate_int32(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewIndex("123", 3600000, "interface", dataPath, testDataInt32, part3.Int32)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInterfaceStoreExtract_int32(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	idx, err := NewIndex("123", 3600000, "interface", dataPath, testDataInt32, part3.Int32)
	if err != nil {
		b.Fatal(err)
	}
	finishTimestamp := testDataInt32[len(testDataInt32)-1].Timestamp()
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

func BenchmarkInterfaceStoreCreate_float32(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewIndex("123", 3600000, "interface", dataPath, testDataFloat32, part3.Float32)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInterfaceStoreExtract_float32(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	idx, err := NewIndex("123", 3600000, "interface", dataPath, testDataFloat32, part3.Float32)
	if err != nil {
		b.Fatal(err)
	}
	finishTimestamp := testDataFloat32[len(testDataFloat32)-1].Timestamp()
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

func BenchmarkInterfaceStoreCreate_float64(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewIndex("123", 3600000, "interface", dataPath, testDataFloat64, part3.Float64)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInterfaceStoreExtract_float64(b *testing.B) {
	prepare(b)
	b.ResetTimer()
	idx, err := NewIndex("123", 3600000, "interface", dataPath, testDataFloat64, part3.Float64)
	if err != nil {
		b.Fatal(err)
	}
	finishTimestamp := testDataFloat64[len(testDataFloat64)-1].Timestamp()
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
