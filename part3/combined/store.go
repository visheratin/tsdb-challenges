package combined

import (
	"encoding/binary"
	"errors"
	"io/ioutil"
	"math"
	"os"
	"path"

	"github.com/visheratin/tsdb-challenges/data"
	"github.com/visheratin/tsdb-challenges/part3"
)

type Store struct {
	path string
}

func (store Store) Insert(dataParts []Elements, dtype part3.DataType) ([]data.Block, error) {
	blocks := make([]data.Block, 0, len(dataParts))
	fpath := path.Join(store.path, "data")
	buf := []byte{}
	for _, d := range dataParts {
		block, bd, err := createBlock(d, dtype)
		if err != nil {
			return nil, err
		}
		buf = append(buf, bd...)
		blocks = append(blocks, block)
	}
	err := ioutil.WriteFile(fpath, buf, 0777)
	if err != nil {
		return nil, err
	}
	return blocks, nil
}

func createBlock(d Elements, dtype part3.DataType) (data.Block, []byte, error) {
	if d.Len() == 0 {
		return data.Block{}, nil, errors.New("data slice is empty")
	}
	l := d.Len()
	var bl int
	switch dtype {
	case part3.Int32:
		bl = 8 * l
	case part3.Float32:
		bl = 8 * l
	case part3.Float64:
		bl = 12 * l
	}
	buf := make([]byte, bl)
	var ts uint32
	var tsp int64
	var t int64
	var f32, f32p, f32d uint32
	var f64, f64p, f64d uint64
	c := 0
	for i := 0; i < l; i++ {
		t = d.Timestamp(i)
		if i > 0 {
			ts = uint32(t - tsp)
		} else {
			ts = uint32(t)
		}
		tsp = t
		binary.LittleEndian.PutUint32(buf[c:c+4], ts)
		c += 4
		switch dtype {
		case part3.Int32:
			f32 = uint32(d.I32[i].Val)
			f32d = f32 - f32p
			f32p = f32
			binary.LittleEndian.PutUint32(buf[c:c+4], f32d)
			c += 4
		case part3.Float32:
			f32 = math.Float32bits(d.F32[i].Val)
			f32d = f32 - f32p
			f32p = f32
			binary.LittleEndian.PutUint32(buf[c:c+4], f32d)
			c += 4
		case part3.Float64:
			f64 = math.Float64bits(d.F64[i].Val)
			f64d = f64 - f64p
			f64p = f64
			binary.LittleEndian.PutUint64(buf[c:c+8], f64d)
			c += 8
		}
	}
	b := data.Block{
		ElNum: l,
		Size:  len(buf),
	}
	return b, buf, nil
}

func (store Store) Read(blockIds []int, blockSizes []int, blockNums []int,
	offset int64, dtype part3.DataType) (Elements, error) {
	var totalSize int
	for _, s := range blockSizes {
		totalSize += s
	}
	fpath := path.Join(store.path, "data")
	f, err := os.Open(fpath)
	if err != nil {
		return Elements{}, err
	}
	defer f.Close()
	_, err = f.Seek(offset, 0)
	zb := make([]byte, totalSize)
	var n, i int
	for err == nil && n != totalSize {
		i, err = f.Read(zb[n:])
		n += i
	}
	if err != nil {
		return Elements{}, err
	}
	var totalNum int
	for _, num := range blockNums {
		totalNum += num
	}
	res := Elements{Type: dtype}
	switch dtype {
	case part3.Int32:
		res.I32 = make([]part3.Int32Element, 0, totalNum)
	case part3.Float32:
		res.F32 = make([]part3.Float32Element, 0, totalNum)
	case part3.Float64:
		res.F64 = make([]part3.Float64Element, 0, totalNum)
	}
	pos := int64(0)
	for i, num := range blockNums {
		d, err := store.readBlock(num, zb[pos:(pos+int64(blockSizes[i]))], dtype)
		if err != nil {
			return Elements{}, err
		}
		res.Append(d)
		pos += int64(blockSizes[i])
	}
	return res, nil
}

func (store Store) readBlock(elNum int, bd []byte, dtype part3.DataType) (Elements, error) {
	res := NewElements(dtype, elNum)
	var ts int64
	ec := 0
	i := 0
	var f64 uint64
	var v32 uint32
	var i32e part3.Int32Element
	var f32e part3.Float32Element
	var f64e part3.Float64Element
	var tb, vb []byte
	for i < len(bd) && ec < elNum {
		tb = bd[i : i+4]
		v32 = binary.LittleEndian.Uint32(tb)
		ts += int64(v32)
		i += 4
		switch dtype {
		case part3.Int32:
			vb = bd[i : i+4]
			v32 += binary.LittleEndian.Uint32(vb)
			i += 4
			i32e = part3.Int32Element{
				Ts:  ts,
				Val: int32(v32),
			}
			res.I32[ec] = i32e
		case part3.Float32:
			vb = bd[i : i+4]
			v32 += binary.LittleEndian.Uint32(vb)
			i += 4
			f32e = part3.Float32Element{
				Ts:  ts,
				Val: math.Float32frombits(v32),
			}
			res.F32[ec] = f32e
		case part3.Float64:
			vb = bd[i : i+8]
			f64 += binary.LittleEndian.Uint64(vb)
			i += 8
			f64e = part3.Float64Element{
				Ts:  ts,
				Val: math.Float64frombits(f64),
			}
			res.F64[ec] = f64e
		}
		ec++
	}
	res = res.Subset(0, ec)
	return res, nil
}
