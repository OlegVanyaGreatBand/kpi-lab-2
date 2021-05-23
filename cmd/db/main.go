package main

import (
	"fmt"
	"github.com/OlegVanyaGreatBand/kpi-lab-2/cmd/db/datastore"
)

func main() {
	key := "1"
	val := "1"
	db, err := datastore.NewDb("./cmd/db/out", 28)
	if err != nil {
		println(fmt.Sprintf("%s", err))
		return
	}

	err = db.Put(key, val)
	if err != nil {
		println(fmt.Sprintf("%s", err))
		return
	}

	v, err := db.Get(key)
	if err != nil {
		println(fmt.Sprintf("2: %s", err))
		return
	}

	println(v)

	v, err = db.Get(key)
	if err != nil {
		println(fmt.Sprintf("3: %s", err))
		return
	}

	println(v)
}
