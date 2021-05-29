package datastore

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"testing"
)

func TestEntry_Encode(t *testing.T) {
	e1 := entry{
		"key",
		[]byte("value"),
		typeString,
	}
	e1.Decode(e1.Encode())
	if e1.key != "key" {
		t.Error("incorrect key")
	}
	if string(e1.value) != "value" {
		t.Error("incorrect value")
	}
	if e1.valueType != typeString {
		t.Error("incorrect type")
	}

	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, 10)
	e2 := entry{
		key:       "test",
		value:     b,
		valueType: typeInt64,
	}
	e2.Decode(e2.Encode())
	if e2.key != "test" {
		t.Error("incorrect key")
	}
	if int64(binary.LittleEndian.Uint64(e2.value)) != 10 {
		t.Error("incorrect value")
	}
	if e2.valueType != typeInt64 {
		t.Error("incorrect type")
	}
}

func TestReadValue(t *testing.T) {
	e1 := entry{
		key:       "key",
		value:     []byte("test-value"),
		valueType: typeString,
	}
	data := e1.Encode()
	v1, err := readStringValue(bufio.NewReader(bytes.NewReader(data)))
	if err != nil {
		t.Fatal(err)
	}
	if v1 != "test-value" {
		t.Errorf("Got bat value [%s]", v1)
	}

	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, 1209)
	e2 := entry{
		key:       "test",
		value:     b,
		valueType: typeInt64,
	}
	data = e2.Encode()
	v2, err := readInt64Value(bufio.NewReader(bytes.NewReader(data)))
	if err != nil {
		t.Fatal(err)
	}
	if v2 != 1209 {
		t.Errorf("Got bat value [%d]", v2)
	}

	e3 := entry{
		key:       "wrongType",
		value:     []byte("test"),
		valueType: typeInt64,
	}
	data = e3.Encode()
	_, err = readStringValue(bufio.NewReader(bytes.NewReader(data)))
	if err == nil {
		t.Fatalf("Must not parse string with wrong type!")
	}

	e4 := entry{
		key:       "wrongType",
		value:     b,
		valueType: typeString,
	}
	data = e4.Encode()
	_, err = readInt64Value(bufio.NewReader(bytes.NewReader(data)))
	if err == nil {
		t.Fatalf("Must not parse string with wrong type!")
	}
}
