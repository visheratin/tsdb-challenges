package part2

import (
	"bytes"
	"encoding/gob"
	"io/ioutil"
	"path"
	"strconv"

	"github.com/visheratin/tsdb-challenges/data"
)

type GobStore struct {
	path string
}

func (store GobStore) Insert(dataParts [][]data.Element) ([]data.Block, error) {
	blocks := make([]data.Block, len(dataParts))
	for i, d := range dataParts {
		buf := bytes.NewBuffer(nil)
		err := gob.NewEncoder(buf).Encode(d)
		if err != nil {
			return nil, err
		}
		b := buf.Bytes()
		block := data.Block{
			Size:  len(b),
			ElNum: len(d),
		}
		blocks = append(blocks, block)
		fpath := path.Join(store.path, strconv.Itoa(i))
		err = ioutil.WriteFile(fpath, b, 777)
		if err != nil {
			return nil, err
		}
	}
	return blocks, nil
}

func (store GobStore) Read(blockIds []int, blockSizes []int, blockNums []int) ([]data.Element, error) {
	totalNum := 0
	for _, s := range blockSizes {
		totalNum += s
	}
	res := make([]data.Element, 0, totalNum)
	for _, i := range blockIds {
		fpath := path.Join(store.path, strconv.Itoa(i))
		rawData, err := ioutil.ReadFile(fpath)
		if err != nil {
			return nil, err
		}
		var allData []data.Element
		buf := bytes.NewBuffer(rawData)
		err = gob.NewDecoder(buf).Decode(&allData)
		if err != nil {
			return nil, err
		}
		res = append(res, allData...)
	}
	return res, nil
}
