package part3

// Element is an interface that describes timestamp-value pair. Timestamp is
// always int64, whether value can be of different types.
type Element interface {
	Timestamp() int64
	Value() float64
}

// Int32Element is an implementation of Element for time series
// with int32 values.
type Int32Element struct {
	Ts  int64
	Val int32
}

// Timestamp returns timestamp of Int32Element instance.
func (el Int32Element) Timestamp() int64 {
	return el.Ts
}

// Value returns value of Int32Element instance.
func (el Int32Element) Value() float64 {
	return float64(el.Val)
}

// Float32Element is an implementation of Element for time series
// with float32 values.
type Float32Element struct {
	Ts  int64
	Val float32
}

// Timestamp returns timestamp of Float32Element instance.
func (el Float32Element) Timestamp() int64 {
	return el.Ts
}

// Value returns value of Float32Element instance.
func (el Float32Element) Value() float64 {
	return float64(el.Val)
}

// Float64Element is an implementation of Element for time series
// with float64 values.
type Float64Element struct {
	Ts  int64
	Val float64
}

// Timestamp returns timestamp of Float64Element instance.
func (el Float64Element) Timestamp() int64 {
	return el.Ts
}

// Value returns value of Float64Element instance.
func (el Float64Element) Value() float64 {
	return el.Val
}
