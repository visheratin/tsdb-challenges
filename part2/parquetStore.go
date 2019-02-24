package part2

import (
	"io/ioutil"
	"os"
	"path"

	"github.com/xitongsys/parquet-go/ParquetReader"
	"github.com/xitongsys/parquet-go/parquet"

	"github.com/visheratin/tsdb-challenges/data"
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetWriter"
)

type ParquetStore struct {
	path string
}

func (store ParquetStore) Insert(dataParts [][]data.Element) ([]data.Block, error) {
	blocks := make([]data.Block, 0, len(dataParts))
	fpath := path.Join(store.path, "data")
	encoded := []byte{}
	for _, d := range dataParts {
		fw, err := ParquetFile.NewBufferFile(nil)
		if err != nil {
			return nil, err
		}
		pw, err := ParquetWriter.NewParquetWriter(fw, new(data.Element), 4)
		if err != nil {
			return nil, err
		}
		pw.RowGroupSize = 12 * 1024 * 1024
		pw.CompressionType = parquet.CompressionCodec_UNCOMPRESSED
		for _, el := range d {
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
		b := fw.(ParquetFile.BufferFile).Bytes()
		block := data.Block{
			Size:  len(b),
			ElNum: len(d),
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

func (store ParquetStore) Read(blockIds []int, blockSizes []int, blockNums []int, offset int64) ([]data.Element, error) {
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
	res := make([]data.Element, 0, totalNum)
	pos := int64(0)
	for i := range blockIds {
		rawData := zb[pos:(pos + int64(blockSizes[i]))]
		fw, err := ParquetFile.NewBufferFile(rawData)
		if err != nil {
			return nil, err
		}
		pr, err := ParquetReader.NewParquetReader(fw, new(data.Element), 4)
		if err != nil {
			return nil, err
		}
		allData := make([]data.Element, blockNums[i])
		if err = pr.Read(&allData); err != nil {
			return nil, err
		}
		res = append(res, allData...)
		pos += int64(blockSizes[i])
	}
	return res, nil
}
