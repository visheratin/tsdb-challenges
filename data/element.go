package data

type Element struct {
	Timestamp int64   `parquet:"name=ts, type=INT64"`
	Value     float64 `parquet:"name=v, type=DOUBLE"`
}
