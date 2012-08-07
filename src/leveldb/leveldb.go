package leveldb

/*
#cgo LDFLAGS: -lleveldb

#include <stdlib.h>
#include <leveldb/c.h>
*/
import "C"

import (
	"unsafe"
)


type Db struct {
	ldb    *C.leveldb_t
}

type Cache struct {
	ldb_cache    *C.leveldb_cache_t
}

type Comparator struct {
	ldb_comparator    *C.leveldb_comparator_t
}

type Env struct {
	ldb_env    *C.leveldb_env_t
}

type Fileterpolicy struct {
	ldb_filterpolicy    *C.leveldb_filterpolicy_t
}

type Iterator struct {
	ldb_iterator    *C.leveldb_iterator_t
}

type Logger struct {
	ldb_comparator    *C.leveldb_logger_t
}

type Options struct {
	ldb_options    *C.leveldb_options_t
}

type Readoptions struct {
	ldb_readoptions    *C.leveldb_readoptions_t
}

type Snapshot struct {
	ldb_options    *C.leveldb_snapshot_t
}

type Writebatch struct {
	ldb_writebatch    *C.leveldb_writebatch_t
}

type Writeoptions struct {
	ldb_writeoptions    *C.leveldb_writeoptions_t
}

type ldb_error string
func (e ldb_error) Error() string {
        return string(e)
}


func Open(options *Options, name string)  (*Db, error) {
	var db_err *C.char

	db_name := C.CString(name)
	defer C.free(unsafe.Pointer(db_name))

	ldb := C.leveldb_open(options.ldb_options, db_name, &db_err)
	if ldb == nil {
		return nil, ldb_error(C.GoString(db_err))
	}

	return &Db{ldb}, nil
}

func (ldb *Db) Put(w_options *Writeoptions, key, value []byte) error {
	var db_err *C.char
	
	key_len := len(key)
	value_len := len(value)
	if key_len == 0 {
		return ldb_error("key must be not empty")
	}

	if value_len == 0 {
		return ldb_error("value must be not empty")
	}
	
	key_c := (*C.char) (unsafe.Pointer(&key[0]))
	value_c := (*C.char) (unsafe.Pointer(&value[0]))
	C.leveldb_put(ldb.ldb, w_options.ldb_writeoptions,
		key_c, C.size_t(key_len),
		value_c, C.size_t(value_len), &db_err)
	if db_err != nil {
		return ldb_error(C.GoString(db_err))
	}
	return nil
}

func (ldb *Db) Get(read_option *Readoptions, key []byte) ([]byte, error) {
	var db_err *C.char
	var value_len C.size_t

	key_len := len(key)
	if key_len == 0 {
		return nil, ldb_error("key must not empty");
	}

	key_c := (*C.char) (unsafe.Pointer(&key[0]))
	value := C.leveldb_get(ldb.ldb, read_option.ldb_readoptions, key_c, C.size_t(key_len),
		&value_len, &db_err)

	if db_err != nil {
		return nil, ldb_error(C.GoString(db_err))
	}

	return C.GoBytes(unsafe.Pointer(value), C.int(value_len)), nil
}

func (ldb* Db) Close() {
	C.leveldb_close(ldb.ldb)
}
