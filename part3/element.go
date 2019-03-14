package part3

type Element interface {
	Timestamp() int64
	Value() float64
}

type Int32Element struct {
	Ts  int64
	Val int32
}

func (el Int32Element) Timestamp() int64 {
	return el.Ts
}

func (el Int32Element) Value() float64 {
	return float64(el.Val)
}

type Float32Element struct {
	Ts  int64
	Val float32
}

func (el Float32Element) Timestamp() int64 {
	return el.Ts
}

func (el Float32Element) Value() float64 {
	return float64(el.Val)
}

type Float64Element struct {
	Ts  int64
	Val float64
}

func (el Float64Element) Timestamp() int64 {
	return el.Ts
}

func (el Float64Element) Value() float64 {
	return el.Val
}
