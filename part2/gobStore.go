package part2

import (
	"bytes"
	"encoding/gob"
	"io/ioutil"
	"os"
	"path"

	"github.com/visheratin/tsdb-challenges/data"
)

type GobStore struct {
	path string
}

func (store GobStore) Insert(dataParts []Elements) ([]data.Block, error) {
	blocks := make([]data.Block, 0, len(dataParts))
	fpath := path.Join(store.path, "data")
	encoded := []byte{}
	for _, d := range dataParts {
		buf := bytes.NewBuffer(nil)
		err := gob.NewEncoder(buf).Encode(d.Data)
		if err != nil {
			return nil, err
		}
		b := buf.Bytes()
		block := data.Block{
			Size:  len(b),
			ElNum: len(d.Data),
		}
		encoded = append(encoded, b...)
		blocks = append(blocks, block)

	}
	err := ioutil.WriteFile(fpath, encoded, 0777)
	if err != nil {
		return nil, err
	}
	return blocks, nil
}

func (store GobStore) Read(blockIds []int, blockSizes []int, blockNums []int, offset int64) (Elements, error) {
	totalNum := 0
	for _, s := range blockNums {
		totalNum += s
	}
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
	res := make([]Element, 0, totalNum)
	pos := int64(0)
	for i := range blockNums {
		rawData := zb[pos:(pos + int64(blockSizes[i]))]
		var allData []Element
		buf := bytes.NewBuffer(rawData)
		err = gob.NewDecoder(buf).Decode(&allData)
		if err != nil {
			return Elements{}, err
		}
		res = append(res, allData...)
		pos += int64(blockSizes[i])
	}
	return Elements{Data: res}, nil
}
