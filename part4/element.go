package part4

type Number interface {
	int32 | float32 | float64
}

func Value[N Number](n N) float64 {
	return float64(n)
}

type Element[ValueType Number] struct {
	Timestamp int64
	Val       ValueType
}
