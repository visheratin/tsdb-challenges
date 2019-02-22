package part2

import (
	"path"
	"strconv"

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
	blocks := make([]data.Block, len(dataParts))
	for i, d := range dataParts {
		fpath := path.Join(store.path, strconv.Itoa(i))
		fw, err := ParquetFile.NewLocalFileWriter(fpath)
		if err != nil {
			return nil, err
		}
		pw, err := ParquetWriter.NewParquetWriter(fw, new(data.Element), 4)
		if err != nil {
			return nil, err
		}
		pw.RowGroupSize = 12 * 1024 * 1024
		pw.CompressionType = parquet.CompressionCodec_GZIP
		for _, el := range d {
			if err = pw.Write(el); err != nil {
				return nil, err
			}
		}
		if err = pw.WriteStop(); err != nil {
			return nil, err
		}
		err = fw.Close()
		if err != nil {
			return nil, err
		}
	}
	return blocks, nil
}

func (store ParquetStore) Read(blockIds []int, blockSizes []int, blockNums []int) ([]data.Element, error) {
	totalNum := 0
	for _, s := range blockSizes {
		totalNum += s
	}
	res := make([]data.Element, 0, totalNum)
	for i, idx := range blockIds {
		fpath := path.Join(store.path, strconv.Itoa(idx))
		fr, err := ParquetFile.NewLocalFileReader(fpath)
		if err != nil {
			return nil, err
		}
		pr, err := ParquetReader.NewParquetReader(fr, new(data.Element), 4)
		if err != nil {
			return nil, err
		}
		allData := make([]data.Element, blockNums[i])
		if err = pr.Read(&allData); err != nil {
			return nil, err
		}
		res = append(res, allData...)
	}
	return res, nil
}
