package protobuf

import (
	"errors"
	"io/ioutil"
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
	buf := make([]byte, 0)
	for _, d := range dataParts {
		block, bd, err := createBlock(d)
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

func createBlock(d ProtoElements) (data.Block, []byte, error) {
	if len(d.Data) == 0 {
		return data.Block{}, nil, errors.New("data slice is empty")
	}
	var buf []byte
	var err error
	buf, err = d.Marshal()
	if err != nil {
		return data.Block{}, nil, err
	}
	b := data.Block{
		ElNum: len(d.Data),
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
	for i := range blockNums {
		var d ProtoElements
		err = d.Unmarshal(zb[pos:(pos + int64(blockSizes[i]))])
		if err != nil {
			return ProtoElements{}, err
		}
		res.Data = append(res.Data, d.Data...)
		pos += int64(blockSizes[i])
	}
	return res, nil
}
