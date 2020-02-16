package part2

import (
	"math/rand"
	"os"
	"testing"
	"time"
)

var gobPath = "./data-gob"
var parquetPath = "./data-parquet"
var protoPath = "./data-proto"
var binaryPath = "./data-binary"

type bench struct {
	name  string
	sType string
	path  string
	data  []Element
}

var benches = []bench{
	{"gob small", "gob", gobPath, testDataSmall},
	{"gob medium", "gob", gobPath, testDataMedium},
	{"gob large", "gob", gobPath, testDataLarge},
	{"parquet small", "parquet", parquetPath, testDataSmall},
	{"parquet medium", "parquet", parquetPath, testDataMedium},
	{"parquet large", "parquet", parquetPath, testDataLarge},
	{"proto small", "proto", protoPath, testDataSmall},
	{"proto medium", "proto", protoPath, testDataMedium},
	{"proto large", "proto", protoPath, testDataLarge},
	{"binary small", "binary", binaryPath, testDataSmall},
	{"binary medium", "binary", binaryPath, testDataMedium},
	{"binary large", "binary", binaryPath, testDataLarge},
}

func BenchmarkStoreCreate(b *testing.B) {
	for _, bs := range benches {
		err := os.MkdirAll(bs.path, 0777)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(bs.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := NewIndex("123", 3600000, bs.sType, bs.path, bs.data)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkStoreExtract(b *testing.B) {
	for _, bs := range benches {
		err := os.MkdirAll(bs.path, 0777)
		if err != nil {
			b.Fatal(err)
		}
		idx, err := NewIndex("123", 3600000, bs.sType, bs.path, bs.data)
		if err != nil {
			b.Fatal(err)
		}
		finishTimestamp := bs.data[len(bs.data)-1].Timestamp
		minStart := finishTimestamp - int64(500000000)
		b.Run(bs.name, func(b *testing.B) {
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
		})
	}
}
