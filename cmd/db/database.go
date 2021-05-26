package main

import (
	"encoding/json"
	"flag"
	"github.com/OlegVanyaGreatBand/kpi-lab-2/cmd/db/datastore"
	"github.com/OlegVanyaGreatBand/kpi-lab-2/httptools"
	"github.com/OlegVanyaGreatBand/kpi-lab-2/signal"
	"log"
	"net/http"
	"strings"
)

var dbDir = flag.String("dir", ".", "database directory")
var port = flag.Int("port", 8070, "database server port")

func main() {
	flag.Parse()
	db, err := datastore.NewDb(*dbDir)
	if err != nil {
		log.Fatalf("Failed to start database: %s", err)
	}
	db.Put("test", "gav")
	log.Printf("Database started at directory: %s", *dbDir)

	h := new(http.ServeMux)
	h.HandleFunc("/db/", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "application/json")
		k := strings.Split(r.URL.Path, "/")[2]
		encoder := json.NewEncoder(rw)

		if r.Method == http.MethodGet {
			log.Printf("GET request for %s", k)
			v, err := db.Get(k)
			if err != nil {
				log.Printf("Failed to get %s: %s", k, err)
				if err == datastore.ErrNotFound {
					rw.WriteHeader(http.StatusNotFound)
				} else {
					rw.WriteHeader(http.StatusInternalServerError)
				}
				return
			}

			res := struct {
				Key string `json:"key"`
				Value string `json:"value"`
			}{
				Key: k,
				Value: v,
			}
			rw.WriteHeader(http.StatusOK)
			if err := encoder.Encode(res); err != nil {
				log.Printf("Failed to write response %s: %s", v, err)
			}
		} else if r.Method == http.MethodPost {
			log.Printf("POST request for %s", k)
			stringValue := struct {
				Value string `json:"value"`
			}{}
			if err := json.NewDecoder(r.Body).Decode(&stringValue); err != nil {
				log.Printf("Error decoding input: %s", err)
				rw.WriteHeader(http.StatusBadRequest)
				return
			}

			if err := db.Put(k, stringValue.Value); err != nil {
				rw.WriteHeader(http.StatusInternalServerError)
				log.Printf("Failed to set %s: %s", k, err)
				return
			}

			rw.WriteHeader(http.StatusOK)
		}
	})

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}
