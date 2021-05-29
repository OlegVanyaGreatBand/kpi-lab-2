package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
)

type entry struct {
	key string
	value []byte
	valueType uint16
}

var ErrWrongType = fmt.Errorf("wrong value type")

func (e *entry) Encode() []byte {
	kl := len(e.key)
	vl := len(e.value)
	size := kl + vl + 14
	res := make([]byte, size)
	binary.LittleEndian.PutUint32(res, uint32(size))
	binary.LittleEndian.PutUint32(res[4:], uint32(kl))
	copy(res[8:], e.key)
	binary.LittleEndian.PutUint32(res[kl+8:], uint32(vl))
	binary.LittleEndian.PutUint16(res[kl+12:], e.valueType)
	copy(res[kl+14:], e.value)
	return res
}

func (e *entry) Decode(input []byte) {
	kl := binary.LittleEndian.Uint32(input[4:])
	keyBuf := make([]byte, kl)
	copy(keyBuf, input[8:kl+8])
	e.key = string(keyBuf)

	vl := binary.LittleEndian.Uint32(input[kl+8:])
	e.valueType = binary.LittleEndian.Uint16(input[kl+12:kl+14])
	valBuf := make([]byte, vl)
	copy(valBuf, input[kl+14:kl+14+vl])
}

func readStringValue(in *bufio.Reader) (string, error) {
	header, err := in.Peek(8)
	if err != nil {
		return "", err
	}
	keySize := int(binary.LittleEndian.Uint32(header[4:]))
	_, err = in.Discard(keySize + 8)
	if err != nil {
		return "", err
	}

	header, err = in.Peek(4)
	if err != nil {
		return "", err
	}
	valSize := int(binary.LittleEndian.Uint32(header))
	_, err = in.Discard(4)
	if err != nil {
		return "", err
	}

	header, err = in.Peek(2)
	if err != nil {
		return "", err
	}

	valType := binary.LittleEndian.Uint16(header)
	if valType != typeString {
		return "", ErrWrongType
	}

	_, err = in.Discard(2)
	if err != nil {
		return "", err
	}

	data := make([]byte, valSize)
	n, err := in.Read(data)
	if err != nil {
		return "", err
	}
	if n != valSize {
		return "", fmt.Errorf("can't read value bytes (read %d, expected %d)", n, valSize)
	}

	return string(data), nil
}


func readInt64Value(in *bufio.Reader) (int64, error) {
	header, err := in.Peek(8)
	if err != nil {
		return 0, err
	}
	keySize := int(binary.LittleEndian.Uint32(header[4:]))
	_, err = in.Discard(keySize + 8)
	if err != nil {
		return 0, err
	}

	header, err = in.Peek(4)
	if err != nil {
		return 0, err
	}
	valSize := int(binary.LittleEndian.Uint32(header))

	_, err = in.Discard(4)
	if err != nil {
		return 0, err
	}

	header, err = in.Peek(2)
	if err != nil {
		return 0, err
	}

	valType := int(binary.LittleEndian.Uint16(header))
	if valType != typeInt64 {
		return 0, ErrWrongType
	}

	_, err = in.Discard(2)
	if err != nil {
		return 0, err
	}

	data := make([]byte, valSize)
	n, err := in.Read(data)
	if err != nil {
		return 0, err
	}
	if n != valSize {
		return 0, fmt.Errorf("can't read value bytes (read %d, expected %d)", n, valSize)
	}

	return int64(binary.LittleEndian.Uint64(data)), nil
}
