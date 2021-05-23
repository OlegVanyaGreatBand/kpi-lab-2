package datastore

import (
"io/ioutil"
"os"
"path/filepath"
	"strings"
	"testing"
)

var testSegSize int64 = 128

func TestDb_Put(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(dir, testSegSize)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pairs := [][]string {
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	outFile, err := os.Open(filepath.Join(dir, segFileName+ "0"))
	if err != nil {
		t.Fatal(err)
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	outInfo, err := outFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	size1 := outInfo.Size()

	t.Run("file growth", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		outInfo, err := outFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if size1 * 2 != outInfo.Size() {
			t.Errorf("Unexpected size (%d vs %d)", size1, outInfo.Size())
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(dir, testSegSize)
		if err != nil {
			t.Fatal(err)
		}

		for _, pair := range pairs {
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	t.Run("segmentation", func(t *testing.T) {
		longKey := "long"
		longVal := strings.Repeat("value", 20)

		if err = db.Put(longKey, longVal); err != nil {
			t.Errorf("Cannot put long key: %s", err)
		}
		if _, err = os.Open(filepath.Join(dir, segFileName + "1")); err != nil {
			t.Errorf("Cannot read segment file: %s", err)
		}

		value, err := db.Get(longKey)
		if err != nil {
			t.Errorf("Cannot read long value: %s", err)
		}
		if value != longVal {
			t.Errorf("Bad value returned expected %s, got %s", longVal, value)
		}
	})

	t.Run("merge", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(dir, 64)
		if err != nil {
			t.Fatal(err)
		}

		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}

		if _, err = os.Open(filepath.Join(dir, segFileName + "0")); err != nil {
			t.Errorf("Cannot read segment file: %s", err)
		}
		if _, err = os.Open(filepath.Join(dir, segFileName + "1")); err == nil {
			t.Errorf("Segment was not merged!: %s", err)
		}
		if _, err = os.Open(filepath.Join(dir, segFileName + "2")); err != nil {
			t.Errorf("Cannot read segment file: %s", err)
		}
	})
}
