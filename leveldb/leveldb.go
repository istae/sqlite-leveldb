package leveldb

import (
	"encoding/binary"
	"errors"

	"github.com/syndtr/goleveldb/leveldb"
)

type Uint64Field struct {
	DB  *leveldb.DB
	Key []byte
}

func New() *leveldb.DB {
	ldb, err := leveldb.OpenFile("leveldb.db", nil)
	if err != nil {
		panic(err)
	}

	return ldb
}

func (f Uint64Field) Put(val uint64) (err error) {
	return f.DB.Put(f.Key, encodeUint64(val), nil)
}

func (f Uint64Field) Get() (val uint64, err error) {
	b, err := f.DB.Get(f.Key, nil)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return binary.BigEndian.Uint64(b), nil
}

func encodeUint64(val uint64) (b []byte) {
	b = make([]byte, 8)
	binary.BigEndian.PutUint64(b, val)
	return b
}
