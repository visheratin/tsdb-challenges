package protobuf

import (
	"encoding/binary"
	"errors"
	"io/ioutil"
	"math"
	"os"
	"path"

	"github.com/visheratin/tsdb-challenges/data"
)

type ProtoStore struct {
	path string
}

func (store ProtoStore) Insert(dataParts []ProtoElements) ([]data.Block, error) {
	blocks := make([]data.Block, 0, len(dataParts))
	fpath := path.Join(store.path, "data")
	var dl int
	for _, d := range dataParts {
		dl += 12 * len(d.Data)
	}
	buf := make([]byte, 0, dl)
	for _, d := range dataParts {
		block, bd, err := createBlock(d.Data)
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

func createBlock(d []ProtoElement) (data.Block, []byte, error) {
	if len(d) == 0 {
		return data.Block{}, nil, errors.New("data slice is empty")
	}
	bl := 12 * len(d)
	buf := make([]byte, bl)
	var ts uint32
	var tsp int64
	var t int64
	var f64, f64p, f64d uint64
	l := len(d)
	c := 0
	for i := 0; i < l; i++ {
		t = d[i].Timestamp
		f64 = math.Float64bits(d[i].Value)
		f64d = f64 - f64p
		f64p = f64
		if i > 0 {
			ts = uint32(t - tsp)
		} else {
			ts = uint32(t)
		}
		tsp = t
		binary.LittleEndian.PutUint32(buf[c:c+4], ts)
		c += 4
		binary.LittleEndian.PutUint64(buf[c:c+8], uint64(f64d))
		c += 8
	}
	b := data.Block{
		ElNum: len(d),
		Size:  len(buf),
	}
	return b, buf, nil
}

func (store ProtoStore) Read(blockIds []int, blockSizes []int, blockNums []int, offset int64) (ProtoElements, error) {
	var totalSize int
	for _, s := range blockSizes {
		totalSize += s
	}
	fpath := path.Join(store.path, "data")
	f, err := os.Open(fpath)
	if err != nil {
		return ProtoElements{}, err
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
		return ProtoElements{}, err
	}
	var totalNum int
	for _, num := range blockNums {
		totalNum += num
	}
	res := ProtoElements{Data: make([]ProtoElement, 0, totalNum)}
	pos := int64(0)
	for i, num := range blockNums {
		d, err := readBlock(num, zb[pos:(pos+int64(blockSizes[i]))])
		if err != nil {
			return ProtoElements{}, err
		}
		res.Data = append(res.Data, d...)
		pos += int64(blockSizes[i])
	}
	return res, nil
}

func readBlock(elNum int, bd []byte) ([]ProtoElement, error) {
	var res []ProtoElement
	var ts int64
	res = make([]ProtoElement, elNum)
	ec := 0
	i := 0
	var f64 uint64
	var tsV uint32
	var f64e ProtoElement
	var tb, vb []byte
	for i < len(bd) && ec < elNum {
		tb = bd[i : i+4]
		tsV = binary.LittleEndian.Uint32(tb)
		ts += int64(tsV)
		i += 4
		vb = bd[i : i+8]
		f64 += binary.LittleEndian.Uint64(vb)
		f64v := math.Float64frombits(f64)
		tsv := ts
		i += 8
		f64e = ProtoElement{
			Timestamp: tsv,
			Value:     f64v,
		}
		res[ec] = f64e
		ec++
	}
	res = res[0:ec]
	return res, nil
}