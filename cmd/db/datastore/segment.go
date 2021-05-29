package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

type hashIndex map[string]int64

var ErrNotFound = fmt.Errorf("record does not exist")
var SegmentCorrupted = fmt.Errorf("segment corrupted")

type segment struct {
	path   string
	file   *os.File
	offset int64
	index  hashIndex
}

const bufSize = 8192

func (s *segment) recover() error {
	input, err := os.Open(s.path)
	if err != nil {
		return err
	}
	defer input.Close()

	var buf [bufSize]byte
	in := bufio.NewReaderSize(input, bufSize)
	for err == nil {
		var (
			header, data []byte
			n int
		)
		header, err = in.Peek(bufSize)
		if err == io.EOF {
			if len(header) == 0 {
				return err
			}
		} else if err != nil {
			return err
		}
		size := binary.LittleEndian.Uint32(header)

		if size < bufSize {
			data = buf[:size]
		} else {
			data = make([]byte, size)
		}
		n, err = in.Read(data)

		if err == nil {
			if n != int(size) {
				return fmt.Errorf("corrupted file")
			}

			var e entry
			e.Decode(data)
			s.index[e.key] = s.offset
			s.offset += int64(n)
		}
	}
	return err
}

func (s *segment) close() error {
	return s.file.Close()
}

func (s *segment) put(key, value string) error {
	e := entry{
		key:   key,
		value: []byte(value),
		valueType: typeString,
	}
	n, err := s.file.Write(e.Encode())
	if err == nil {
		s.index[key] = s.offset
		s.offset += int64(n)
	}
	return err
}

func (s *segment) putInt64(key string, value int64) error {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(value))
	e := entry{
		key:   key,
		value: b,
		valueType: typeInt64,
	}
	n, err := s.file.Write(e.Encode())
	if err == nil {
		s.index[key] = s.offset
		s.offset += int64(n)
	}
	return err
}

func (s *segment) get(key string) (string, error) {
	position, ok := s.index[key]
	if !ok {
		return "", ErrNotFound
	}

	file, err := os.Open(s.path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(position, 0)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(file)
	value, err := readStringValue(reader)
	if err != nil {
		return "", err
	}

	return value, nil
}

func (s *segment) getInt64(key string) (int64, error) {
	position, ok := s.index[key]
	if !ok {
		return 0, ErrNotFound
	}

	file, err := os.Open(s.path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	_, err = file.Seek(position, 0)
	if err != nil {
		return 0, err
	}

	reader := bufio.NewReader(file)
	value, err := readInt64Value(reader)
	if err != nil {
		return 0, err
	}

	return value, nil
}

func (s *segment) number() (int, error) {
	name := s.file.Name()
	i := strings.Index(name, segFileName)

	if i == -1 {
		return 0, SegmentCorrupted
	}

	num := name[i + len(segFileName):]
	return strconv.Atoi(num)
}
