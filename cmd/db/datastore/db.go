package datastore

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const segFileName = "segment-"
const defSegSize = 10485760 // 10 Mb
var autoMerge = true

const (
	typeString = iota
	typeClose = iota
)

type Db struct {
	sync.RWMutex
	outPath string
	segments []*segment
	maxSegSize int64
	writeQueue chan writeRequest
	mergeQueue chan interface{}
	closed bool
}

type writeRequest struct {
	key string
	value interface{}
	valueType int
	result chan error
}

// NewDb Create new database with default segment size
// Note that creating two dbs in the same directory may lead to data races and data corruption
func NewDb(dir string) (*Db, error) {
	return NewDbSized(dir, defSegSize)
}

// NewDbSized Create new database with provided segment size.
// Note that creating two dbs in the same directory may lead to data races and data corruption
func NewDbSized(dir string, segSize int64) (*Db, error) {
	db := &Db{
		outPath: dir,
		segments: nil,
		maxSegSize: segSize,
		writeQueue: make(chan writeRequest),
		mergeQueue: make(chan interface{}),
	}

	err := db.recover()
	if err != nil {
		return nil, err
	}

	if autoMerge {
		go db.mergeLoop()
	}
	go db.loop()

	return db, nil
}

func (db *Db) loop() {
	for e := range db.writeQueue {
		var err error
		switch e.valueType {
		case typeString:
			err = db.lastSegment().put(e.key, e.value.(string))
		case typeClose:
			return
		}

		db.Lock()
		if err != nil {
			db.Unlock()
			e.result <- err
			continue
		}

		if db.lastSegment().offset >= db.maxSegSize {
			err := db.newSegment()
			db.Unlock()
			e.result <- err
			continue
		}

		db.Unlock()
		e.result <- nil
	}
}

func (db *Db) mergeLoop() {
	for v := range db.mergeQueue {
		if v == typeClose {
			return
		}
		if len(db.segments) > 2 {
			err := db.merge()
			if err != nil {
				log.Printf("Cannot merge: %s", err)
			}
		}
	}
}

func (db *Db) recover() error {
	files, err := ioutil.ReadDir(db.outPath)
	if err != nil {
		return err
	}

	var segments []*segment
	for _, file := range files {
		if !file.IsDir() && strings.HasPrefix(file.Name(), segFileName) {
			if file.Name() == segFileName + "merged" {
				continue
			}
			path := filepath.Join(db.outPath, file.Name())

			seg, err := createSegment(path)
			if err != nil {
				return err
			}

			segments = append(segments, seg)
		}
	}

	if len(segments) == 0 {
		path := filepath.Join(db.outPath, segFileName + "0")
		f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
		if err != nil {
			return err
		}

		seg := &segment{
			offset: 0,
			path:  path,
			file:  f,
			index: make(hashIndex),
		}
		segments = append(segments, seg)
	}

	db.segments = segments

	return nil
}

func createSegment(path string) (*segment, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}

	seg := &segment{
		offset: 0,
		path:  path,
		file:  f,
		index: make(hashIndex),
	}

	err = seg.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}

	return seg, nil
}

// Close current database.
func (db *Db) Close() error {
	db.writeQueue <- writeRequest{valueType: typeClose}
	if autoMerge {
		db.mergeQueue <- typeClose
	}

	db.closed = true

	for _, s := range db.segments {
		if err := s.close(); err != nil {
			return err
		}
	}
	return nil
}

// Get the value from database.
// This operation may block thread if there is ongoing write operations.
func (db *Db) Get(key string) (string, error) {
	db.RLock()
	defer db.RUnlock()
	for i := len(db.segments) - 1; i >= 0; i-- {
		v, err := db.segments[i].get(key)
		if err == nil {
			return v, nil
		}
	}

	return "", ErrNotFound
}

func (db *Db) lastSegment() *segment {
	return db.segments[len(db.segments) - 1]
}

// Put value to database under the provided key. This is blocking operation
func (db *Db) Put(key, value string) error {
	req := writeRequest{
		key:    key,
		value:  value,
		result: make(chan error),
		valueType: typeString,
	}

	db.writeQueue <- req

	return <- req.result
}

func (db *Db) newSegment() error {
	n, err := db.lastSegment().number()
	if err != nil {
		return err
	}

	path := filepath.Join(db.outPath, fmt.Sprintf("%s%d", segFileName, n + 1))
	seg, err := createSegment(path)
	if err != nil {
		return err
	}

	db.segments = append(db.segments, seg)

	if len(db.segments) > 2 && autoMerge {
		go func() {
			db.mergeQueue <- struct {}{}
		}()
	}

	return nil
}

func (db *Db) merge() error {
	backup := db.segments
	segments := db.segments[:len(db.segments) - 1]

	table := make(map[string]int64)
	path := filepath.Join(db.outPath, fmt.Sprintf("%s%s", segFileName, "merged"))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}

	seg := &segment{
		offset: 0,
		path:  path,
		file:  f,
		index: make(hashIndex),
	}

	for i := len(segments) - 1; i >= 0; i-- {
		s := segments[i]
		for k := range s.index {
			if _, ok := table[k]; ok {
				continue
			}

			v, err := s.get(k)
			if err != nil {
				seg.close()
				os.Remove(path)
				return err
			}

			table[k] = 1

			err = seg.put(k, v)
			if err != nil {
				seg.close()
				os.Remove(path)
				return err
			}
		}
	}

	// DANGER ZONE!
	// Beware, stranger, as following code affects db active segments and files and
	// should be synchronized and changed with great awareness.
	db.Lock()
	// Protection from erasing new segments that was created while merge was in progress
	newSegments := append([]*segment{seg}, db.segments[len(segments):]...)
	db.segments = newSegments

	err = os.Rename(path, segments[0].path)
	if err != nil {
		db.segments = backup
		os.Remove(path)
		db.Unlock()
		return err
	}

	f, err = os.OpenFile(segments[0].path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		db.segments = backup
		os.Remove(path)
		db.Unlock()
		return err
	}

	seg.path = segments[0].path
	seg.file = f
	db.Unlock()

	for _, s := range segments {
		s.close()
		if s != segments[0] {
			os.Remove(s.path)
		}
	}

	return nil
}
