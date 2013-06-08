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
	ldb *C.leveldb_t
}

type Cache struct {
	ldb_cache *C.leveldb_cache_t
}

type Comparator struct {
	ldb_comparator *C.leveldb_comparator_t
}

type Env struct {
	ldb_env *C.leveldb_env_t
}

type Fileterpolicy struct {
	ldb_filterpolicy *C.leveldb_filterpolicy_t
}

type Iterator struct {
	ldb_iterator *C.leveldb_iterator_t
}

type Logger struct {
	ldb_comparator *C.leveldb_logger_t
}

type Snapshot struct {
	ldb_options *C.leveldb_snapshot_t
}

type ldb_error string

func (e ldb_error) Error() string {
	return string(e)
}

func Open(name string, options *Options) (*Db, error) {
	var db_err *C.char

	db_name := C.CString(name)
	defer C.free(unsafe.Pointer(db_name))

	ldb := C.leveldb_open(options.options, db_name, &db_err)
	if ldb == nil {
		return nil, ldb_error(C.GoString(db_err))
	}

	return &Db{ldb}, nil
}

func (ldb *Db) Put(key, value []byte, w_options *Writeoptions) error {
	var db_err *C.char

	key_len := len(key)
	value_len := len(value)
	if key_len == 0 {
		return ldb_error("key must be not empty")
	}

	if value_len == 0 {
		return ldb_error("value must be not empty")
	}

	key_c := (*C.char)(unsafe.Pointer(&key[0]))
	value_c := (*C.char)(unsafe.Pointer(&value[0]))
	C.leveldb_put(ldb.ldb, w_options.options,
		key_c, C.size_t(key_len),
		value_c, C.size_t(value_len), &db_err)
	if db_err != nil {
		return ldb_error(C.GoString(db_err))
	}
	return nil
}

func (ldb *Db) Get(key []byte, read_option *Readoptions) ([]byte, error) {
	var db_err *C.char
	var value_len C.size_t

	key_len := len(key)
	if key_len == 0 {
		return nil, ldb_error("key must not empty")
	}

	key_c := (*C.char)(unsafe.Pointer(&key[0]))
	value := C.leveldb_get(ldb.ldb, read_option.options, key_c, C.size_t(key_len),
		&value_len, &db_err)

	if db_err != nil {
		return nil, ldb_error(C.GoString(db_err))
	}

	if value == nil {
		return nil, nil
	}

	return C.GoBytes(unsafe.Pointer(value), C.int(value_len)), nil
}

func (ldb *Db) Delete(key []byte, w_options *Writeoptions) error {
	var db_err *C.char

	key_len := len(key)
	if key_len == 0 {
		return ldb_error("key must not empty")
	}

	key_c := (*C.char)(unsafe.Pointer(&key[0]))
	C.leveldb_delete(ldb.ldb, w_options.options, key_c, C.size_t(key_len), &db_err)
	if db_err != nil {
		return ldb_error(C.GoString(db_err))
	}

	return nil
}

func (ldb *Db) Write(w_options *Writeoptions, batch *Writebatch) error {
	var db_err *C.char

	C.leveldb_write(ldb.ldb, w_options.options, batch.wb, &db_err)
	if db_err != nil {
		return ldb_error(C.GoString(db_err))
	}

	return nil
}

func (ldb *Db) Close() {
	C.leveldb_close(ldb.ldb)
}
