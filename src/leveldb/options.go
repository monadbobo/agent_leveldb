package leveldb

/*
#cgo LDFLAGS: -lleveldb

#include <stdlib.h>
#include <leveldb/c.h>
*/
import "C"

type Options struct {
	options    *C.leveldb_options_t
}

type Readoptions struct {
	options    *C.leveldb_readoptions_t
}

type Writeoptions struct {
	options    *C.leveldb_writeoptions_t
}

func Create_options() *Options {
	option := C.leveldb_options_create()
	return &Options{option}
}

func Create_read_options() *Readoptions {
	option := C.leveldb_readoptions_create()
	return &Readoptions{option}
}

func Create_write_options() *Writeoptions {
	option := C.leveldb_writeoptions_create()
	return &Writeoptions{option}
}

func (option *Options) Destroy_options() {
	C.leveldb_options_destroy(option.options)
}

func (option *Readoptions) Destroy_readoptions() {
	C.leveldb_readoptions_destroy(option.options)
}

func (option *Options) Destroy_writeoptions() {
	C.leveldb_writeoptions_destroy(option.options)
}
