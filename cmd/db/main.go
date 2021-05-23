package main

import (
	"github.com/OlegVanyaGreatBand/kpi-lab-2/cmd/db/datastore"
	"log"
	"strconv"
	"strings"
	"time"
)

func main() {
	db, err := datastore.NewDb("./cmd/db/out",)
	if err != nil {
		log.Fatal(err)
	}

	base := "lalalalalalllllllll"

	ch := make(chan interface{})
	for i := 0; i < 10000; i++ {
		i := i
		go func() {
			time.Sleep(2 * time.Millisecond)
			k := base + strconv.Itoa(i)
			v := base + strings.Repeat(k, 10)
			err := db.Put(k, v)
			if err != nil {
				log.Fatal(err)
			}
			ch <- struct {}{}
		}()
	}

	for i := 0; i < 10000; i++ {
		<- ch
	}

	for i := 0; i < 10000; i++ {
		i := i
		go func() {
			k := strconv.Itoa(i)
			v := strings.Repeat(k, 10)
			val, err := db.Get(k)
			if err != nil {
				log.Fatal(err)
			}
			if val != v {
				log.Fatalf("Expected %s but got %s", k, v)
			}
			ch <- struct {}{}
		}()
	}

	for i := 0; i < 10000; i++ {
		<- ch
	}
	//
	//time.Sleep(5 * time.Second)
}
