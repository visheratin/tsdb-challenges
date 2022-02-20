package part4

import (
	"encoding/binary"
	"errors"
	"io/ioutil"
	"math"
	"os"
	"path"

	"github.com/visheratin/tsdb-challenges/data"
)

// Insert loads slice of data parts, each of which is represented as []Element,
// to the storage and if successful returns a slice of index blocks.
func Insert[N Number](dataParts [][]Element[N], dtype DataType, dataPath string) ([]data.Block, error) {
	blocks := make([]data.Block, 0, len(dataParts))
	fpath := path.Join(dataPath, "data")
	var dl int
	for _, d := range dataParts {
		switch dtype {
		case Int32:
			dl += 8 * len(d)
		case Float32:
			dl += 8 * len(d)
		case Float64:
			dl += 12 * len(d)
		}
	}
	buf := make([]byte, 0, dl)
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

func createBlock[N Number](d []Element[N], dtype DataType) (data.Block, []byte, error) {
	if len(d) == 0 {
		return data.Block{}, nil, errors.New("data slice is empty")
	}
	var bl int
	switch dtype {
	case Int32:
		bl = 8 * len(d)
	case Float32:
		bl = 8 * len(d)
	case Float64:
		bl = 12 * len(d)
	}
	buf := make([]byte, bl)
	var ts uint32
	var tsp int64
	var t int64
	var f32, f32p, f32d uint32
	var f64, f64p, f64d uint64
	l := len(d)
	c := 0
	for i := 0; i < l; i++ {
		t = d[i].Timestamp
		if i > 0 {
			ts = uint32(t - tsp)
		} else {
			ts = uint32(t)
		}
		tsp = t
		binary.LittleEndian.PutUint32(buf[c:c+4], ts)
		c += 4
		switch dtype {
		case Int32:
			f32 = uint32(d[i].Val)
			f32d = f32 - f32p
			f32p = f32
			binary.LittleEndian.PutUint32(buf[c:c+4], f32d)
			c += 4
		case Float32:
			f32 = math.Float32bits(float32(d[i].Val))
			f32d = f32 - f32p
			f32p = f32
			binary.LittleEndian.PutUint32(buf[c:c+4], f32d)
			c += 4
		case Float64:
			f64 = math.Float64bits(float64(d[i].Val))
			f64d = f64 - f64p
			f64p = f64
			binary.LittleEndian.PutUint64(buf[c:c+8], f64d)
			c += 8
		}
	}
	b := data.Block{
		ElNum: len(d),
		Size:  len(buf),
	}
	return b, buf, nil
}

// Read uses meta-information about data blocks to extract data from the store.
func Read[N Number](blockIds []int, blockSizes []int, blockNums []int,
	offset int64, dtype DataType, dataPath string) ([]Element[N], error) {
	var totalSize int
	for _, s := range blockSizes {
		totalSize += s
	}
	fpath := path.Join(dataPath, "data")
	f, err := os.Open(fpath)
	if err != nil {
		return nil, err
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
		return nil, err
	}
	var totalNum int
	for _, num := range blockNums {
		totalNum += num
	}
	res := make([]Element[N], 0, totalNum)
	pos := int64(0)
	for i, num := range blockNums {
		d, err := readBlock[N](num, zb[pos:(pos+int64(blockSizes[i]))], dtype)
		if err != nil {
			return nil, err
		}
		res = append(res, d...)
		pos += int64(blockSizes[i])
	}
	return res, nil
}

func readBlock[N Number](elNum int, bd []byte, dtype DataType) ([]Element[N], error) {
	var res []Element[N]
	var ts int64
	res = make([]Element[N], elNum)
	ec := 0
	i := 0
	var f64 uint64
	var v32 uint32
	var el Element[N]
	var tb, vb []byte
	for i < len(bd) && ec < elNum {
		tb = bd[i : i+4]
		v32 = binary.LittleEndian.Uint32(tb)
		ts += int64(v32)
		i += 4
		switch dtype {
		case Int32:
			vb = bd[i : i+4]
			v32 += binary.LittleEndian.Uint32(vb)
			i += 4
			el = Element[N]{
				Timestamp: ts,
				Val:       N(int32(v32)),
			}
			res[ec] = el
		case Float32:
			vb = bd[i : i+4]
			v32 += binary.LittleEndian.Uint32(vb)
			i += 4
			el = Element[N]{
				Timestamp: ts,
				Val:       N(math.Float32frombits(v32)),
			}
			res[ec] = el
		case Float64:
			vb = bd[i : i+8]
			f64 += binary.LittleEndian.Uint64(vb)
			i += 8
			el = Element[N]{
				Timestamp: ts,
				Val:       N(math.Float64frombits(f64)),
			}
			res[ec] = el
		}
		ec++
	}
	res = res[0:ec]
	return res, nil
}
