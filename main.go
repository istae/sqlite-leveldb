package main

import (
	"encoding/binary"
	"log"
	"os"
	"runtime/pprof"
	"time"

	"sqlite-test/leveldb"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type Uint64Field struct {
	db  *gorm.DB
	key []byte
}

type KeyValue struct {
	Key   []byte `gorm:"index"`
	Value []byte
}

type Item struct {
	Address         []byte
	Data            []byte
	AccessTimestamp int64
	StoreTimestamp  int64
	BinID           uint64
	PinCounter      uint64 // maintains the no of time a chunk is pinned
	Tag             uint32
	BatchID         []byte // postage batch ID
	Index           []byte // postage stamp within-batch: index
	Timestamp       []byte // postage stamp validity
	Sig             []byte // postage stamp signature
	BucketDepth     uint8  // postage batch bucket depth (for collision sets)
	Depth           uint8  // postage batch depth (for size)
	Radius          uint8  // postage batch reserve radius, po upto and excluding which chunks are unpinned
	Immutable       bool   // whether postage batch can be diluted and drained, and indexes overwritten - nullable bool
}

func main() {

	f, err := os.Create("cpu.profile")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	db, err := gorm.Open(sqlite.Open("sqlite.db"), &gorm.Config{
		SkipDefaultTransaction: true,
		PrepareStmt:            true,
	})

	if err != nil {
		panic("failed to connect database")
	}

	db.AutoMigrate(&KeyValue{})

	err = db.Exec("PRAGMA synchronous = OFF").Error
	if err != nil {
		log.Fatal(err)
	}
	err = db.Exec("PRAGMA journal_mode = OFF").Error
	if err != nil {
		log.Fatal(err)
	}
	sqlF := Uint64Field{db: db}

	levelF := leveldb.Uint64Field{DB: leveldb.New()}

	var keys [][]byte
	for i := uint64(0); i < 1000000; i++ {
		keys = append(keys, encodeUint64(i))
	}

	// sqlite
	start := time.Now()
	for i, k := range keys {
		sqlF.key = k
		err := sqlF.Put(uint64(i))
		if err != nil {
			log.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	log.Printf("sqlite write took %s", elapsed)

	start = time.Now()
	for _, k := range keys {
		sqlF.key = k
		_, err := sqlF.Get()
		if err != nil {
			log.Fatal(err)
		}
	}
	elapsed = time.Since(start)
	log.Printf("sqlite read took %s", elapsed)

	// leveldb
	start = time.Now()
	for i, k := range keys {
		levelF.Key = k
		err := levelF.Put(uint64(i))
		if err != nil {
			log.Fatal(err)
		}
	}
	elapsed = time.Since(start)
	log.Printf("leveldb write took %s", elapsed)

	start = time.Now()
	for _, k := range keys {
		levelF.Key = k
		_, err := levelF.Get()
		if err != nil {
			log.Fatal(err)
		}
	}
	elapsed = time.Since(start)
	log.Printf("leveldb read took %s", elapsed)

}

func (f Uint64Field) Get() (val uint64, err error) {
	var ret = KeyValue{Key: f.key}
	f.db.First(&ret)
	return binary.BigEndian.Uint64(ret.Value), nil
}

func (f Uint64Field) Put(val uint64) (err error) {
	return f.db.Create(&KeyValue{Key: f.key, Value: encodeUint64(val)}).Error
}

func encodeUint64(val uint64) (b []byte) {
	b = make([]byte, 8)
	binary.BigEndian.PutUint64(b, val)
	return b
}
