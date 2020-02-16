package part2

import (
	"io/ioutil"
	"os"
	"path"

	"github.com/visheratin/tsdb-challenges/data"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

type ParquetStore struct {
	path string
}

func (store ParquetStore) Insert(dataParts []Elements) ([]data.Block, error) {
	blocks := make([]data.Block, 0, len(dataParts))
	fpath := path.Join(store.path, "data")
	encoded := []byte{}
	for _, d := range dataParts {
		fw, err := buffer.NewBufferFile(nil)
		if err != nil {
			return nil, err
		}
		pw, err := writer.NewParquetWriter(fw, new(data.Element), 4)
		if err != nil {
			return nil, err
		}
		pw.RowGroupSize = 12 * 1024 * 1024
		pw.CompressionType = parquet.CompressionCodec_UNCOMPRESSED
		for _, el := range d.Data {
			if err = pw.Write(el); err != nil {
				return nil, err
			}
		}
		if err := pw.WriteStop(); err != nil {
			return nil, err
		}
		err = fw.Close()
		if err != nil {
			return nil, err
		}
		b := fw.(buffer.BufferFile).Bytes()
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

func (store ParquetStore) Read(blockIds []int, blockSizes []int, blockNums []int, offset int64) (Elements, error) {
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
	for i := range blockIds {
		rawData := zb[pos:(pos + int64(blockSizes[i]))]
		fw, err := buffer.NewBufferFile(rawData)
		if err != nil {
			return Elements{}, err
		}
		pr, err := reader.NewParquetReader(fw, new(data.Element), 4)
		if err != nil {
			return Elements{}, err
		}
		allData := make([]Element, blockNums[i])
		if err = pr.Read(&allData); err != nil {
			return Elements{}, err
		}
		res = append(res, allData...)
		pos += int64(blockSizes[i])
	}
	return Elements{Data: res}, nil
}
