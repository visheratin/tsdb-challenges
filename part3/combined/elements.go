package combined

import (
	"github.com/visheratin/tsdb-challenges/part3"
)

// Elements is a combined data structure that is used for efficient but combersome processing of
// time series of various types.
type Elements struct {
	Type part3.DataType
	I32  []part3.Int32Element
	F32  []part3.Float32Element
	F64  []part3.Float64Element
}

// NewElements creates Elements instance and preallocates required slice of size len based on
// dtype parameter.
func NewElements(dtype part3.DataType, len int) Elements {
	el := Elements{}
	switch dtype {
	case part3.Int32:
		el.I32 = make([]part3.Int32Element, len)
	case part3.Float32:
		el.F32 = make([]part3.Float32Element, len)
	case part3.Float64:
		el.F64 = make([]part3.Float64Element, len)
	}
	el.Type = dtype
	return el
}

// Len returns length of the data slice in use based on el.Type.
func (el Elements) Len() int {
	switch el.Type {
	case part3.Int32:
		return len(el.I32)
	case part3.Float32:
		return len(el.F32)
	case part3.Float64:
		return len(el.F64)
	default:
		return -1
	}
}

// Timestamp returns timestamp on i-th position in the data slice in use.
func (el Elements) Timestamp(i int) int64 {
	switch el.Type {
	case part3.Int32:
		return el.I32[i].Ts
	case part3.Float32:
		return el.F32[i].Ts
	case part3.Float64:
		return el.F64[i].Ts
	default:
		return int64(-1)
	}
}

// Subset returns values of the data slice in use from sIdx to fIdx.
func (el Elements) Subset(sIdx, fIdx int) Elements {
	res := Elements{}
	switch el.Type {
	case part3.Int32:
		res.I32 = el.I32[sIdx:fIdx]
	case part3.Float32:
		res.F32 = el.F32[sIdx:fIdx]
	case part3.Float64:
		res.F64 = el.F64[sIdx:fIdx]
	}
	res.Type = el.Type
	return res
}

// Append puts values of d into the data slice in use based on el.Type.
func (el *Elements) Append(d Elements) {
	switch el.Type {
	case part3.Int32:
		el.I32 = append(el.I32, d.I32...)
	case part3.Float32:
		el.F32 = append(el.F32, d.F32...)
	case part3.Float64:
		el.F64 = append(el.F64, d.F64...)
	}
}
