package datastore

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const segFileName = "segment-"

type Db struct {
	outPath string
	segments []*segment
	maxSegSize int64
}

func NewDb(dir string, segSize int64) (*Db, error) {
	db := &Db{
		outPath: dir,
		segments: nil,
		maxSegSize: segSize,
	}

	err := db.recover()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (db *Db) recover() error {
	files, err := os.ReadDir(db.outPath)
	if err != nil {
		return err
	}

	var segments []*segment
	for _, file := range files {
		if !file.IsDir() && strings.HasPrefix(file.Name(), segFileName) {
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

func (db *Db) Close() error {
	for _, s := range db.segments {
		if err := s.close(); err != nil {
			return err
		}
	}
	return nil
}

func (db *Db) Get(key string) (string, error) {
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

func (db *Db) Put(key, value string) error {
	err := db.lastSegment().put(key, value)
	if err != nil {
		return err
	}

	if db.lastSegment().offset >= db.maxSegSize {
		err := db.newSegment()
		return err
	}

	return nil
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

	if len(db.segments) > 2 {
		return db.merge()
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
	// In case of errors db may be corrupted so recovery() call may be needed
	db.segments = []*segment{seg, db.lastSegment()}

	err = os.Rename(path, segments[0].path)
	if err != nil {
		db.segments = backup
		os.Remove(path)
		return err
	}

	f, err = os.OpenFile(segments[0].path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		db.segments = backup
		os.Remove(path)
		return err
	}
	seg.path = segments[0].path
	seg.file = f

	for _, s := range segments {
		s.close()
		if s != segments[0] {
			os.Remove(s.path)
		}
	}

	return nil
}
