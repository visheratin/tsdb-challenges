package part4

// Number is a generic interface defining numeric value types.
type Number interface {
	int32 | float32 | float64
}

// Element is a generic type where Value has generic type Number.
type Element[ValueType Number] struct {
	Timestamp int64
	Value     ValueType
}
