package leveldb

import (
	"encoding/binary"

	"github.com/syndtr/goleveldb/leveldb"
)

func New(path string) *leveldb.DB {
	ldb, err := leveldb.OpenFile(path, nil)
	if err != nil {
		panic(err)
	}

	return ldb
}

func Put(db *leveldb.DB, key, val []byte) (err error) {
	return db.Put(key, val, nil)
}

func Get(db *leveldb.DB, key []byte) ([]byte, error) {
	val, err := db.Get(key, nil)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func encodeUint64(val uint64) (b []byte) {
	b = make([]byte, 8)
	binary.BigEndian.PutUint64(b, val)
	return b
}
