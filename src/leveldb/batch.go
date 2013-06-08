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

type Writebatch struct {
	wb *C.leveldb_writebatch_t
}

func New_writebatch() *Writebatch {
	wb := C.leveldb_writebatch_create()
	return &Writebatch{wb}
}

func (wb *Writebatch) Put(key, value []byte) error {
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

	C.leveldb_writebatch_put(wb.wb, key_c, C.size_t(key_len), value_c, C.size_t(value_len))
	return nil
}

func (wb *Writebatch) Delete(key []byte) error {
	var db_err *C.char

	key_len := len(key)
	if key_len == 0 {
		return ldb_error("key must not empty")
	}

	key_c := (*C.char)(unsafe.Pointer(&key[0]))
	C.leveldb_writebatch_delete(wb.wb, key_c, C.size_t(key_len))
	if db_err != nil {
		return ldb_error(C.GoString(db_err))
	}

	return nil
}
