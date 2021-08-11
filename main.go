package main

import (
	"encoding/binary"
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"runtime/pprof"
	"time"

	testleveldb "sqlite-test/leveldb"

	"github.com/syndtr/goleveldb/leveldb"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

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

	os.MkdirAll("db", os.ModePerm)

	db, err := gorm.Open(sqlite.Open("./db/sqlite"), &gorm.Config{
		SkipDefaultTransaction: true,
		PrepareStmt:            true,
		Logger:                 logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		log.Fatal(err)
	}

	err = db.AutoMigrate(&KeyValue{})
	if err != nil {
		log.Fatal(err)
	}

	err = db.Exec("PRAGMA synchronous = OFF").Error
	if err != nil {
		log.Fatal(err)
	}
	err = db.Exec("PRAGMA journal_mode = OFF").Error
	if err != nil {
		log.Fatal(err)
	}

	lvldb := testleveldb.New("db/leveldb")

	rnd := rand.New(rand.NewSource(0))

	keysLen := 5000000
	var keys []KeyValue
	for i := 0; i < keysLen; i++ {
		keys = append(keys,
			KeyValue{
				Key:   encodeUint64(uint64(i)),
				Value: encodeUint64(uint64(i)),
			},
		)
	}
	keySize := len(keys[0].Value)

	itemsLen := 5000000
	var items []KeyValue
	for i := 0; i < itemsLen; i++ {

		item := newItem(rnd)
		data, err := json.Marshal(item)
		if err != nil {
			log.Fatal(err)
		}

		items = append(items, KeyValue{
			Key:   item.Address,
			Value: data,
		})
	}
	itemSize := len(items[0].Value)

	log.Printf("%d keys, value size %d", keysLen, keySize)
	run(db, lvldb, keys, "key-value")
	log.Printf("%d keys, value size %d", itemsLen, itemSize)
	run(db, lvldb, items, "items")
}

func run(db *gorm.DB, lvldb *leveldb.DB, keys []KeyValue, label string) {
	// sqlite
	start := time.Now()
	for _, k := range keys {
		err := Put(db, &k)
		if err != nil {
			log.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	log.Printf("sqlite %s write took %s", label, elapsed)

	start = time.Now()
	for _, k := range keys {
		err := Get(db, &KeyValue{Key: k.Key})
		if err != nil {
			log.Fatal(err)
		}
	}
	elapsed = time.Since(start)
	log.Printf("sqlite %s read took %s", label, elapsed)

	// leveldb
	start = time.Now()
	for _, k := range keys {
		err := testleveldb.Put(lvldb, k.Key, k.Value)
		if err != nil {
			log.Fatal(err)
		}
	}
	elapsed = time.Since(start)
	log.Printf("leveldb %s write took %s", label, elapsed)

	start = time.Now()
	for _, k := range keys {
		_, err := testleveldb.Get(lvldb, k.Key)
		if err != nil {
			log.Fatal(err)
		}
	}
	elapsed = time.Since(start)
	log.Printf("leveldb %s read took %s", label, elapsed)
}

func Get(db *gorm.DB, ret interface{}) error {
	return db.First(ret).Error
}

func Put(db *gorm.DB, value interface{}) error {
	return db.Create(value).Error
}

func encodeUint64(val uint64) (b []byte) {
	b = make([]byte, 8)
	binary.BigEndian.PutUint64(b, val)
	return b
}

func newItem(rnd *rand.Rand) *Item {

	data := make([]byte, 8)
	_, _ = rnd.Read(data)

	return &Item{
		Address:   data,
		BatchID:   data,
		Index:     data,
		Timestamp: data,
		Sig:       data,
	}
}
