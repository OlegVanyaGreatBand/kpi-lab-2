package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/OlegVanyaGreatBand/kpi-lab-2/httptools"
	"github.com/OlegVanyaGreatBand/kpi-lab-2/signal"
)

var (
	port = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

type Server struct {
	name string
	isHealthy bool
}

var (
	timeout = time.Duration(*timeoutSec) * time.Second
	serversPool = []Server{
		{
			name: "server1:8080",
			isHealthy: true,
		},
		{
			name: "server2:8080",
			isHealthy: true,
		},
		{
			name: "server3:8080",
			isHealthy: true,
		},
	}
)

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func health(dst string) bool {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, _ := context.WithTimeout(r.Context(), timeout)
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", dst)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL)
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

func hash(ip string) (uint32, error) {
	host := ip

	if pos := strings.IndexByte(ip, ':'); pos != -1 {
		host = ip[:pos]
	}

	var sum uint32 = 0
	octets := strings.Split(host, ".")
	if len(octets) < 4 {
		return 0, errors.New(fmt.Sprintf("invalid ip %s", ip))
	}
	for i, octet := range octets {
		n, e := strconv.Atoi(octet)
		if e != nil {
			return 0, e
		}

		if n > 255 {
			return 0, errors.New(fmt.Sprintf("invalid ip %s", ip))
		}

		sum += uint32(n) << (i * 8)
	}

	return sum, nil
}

func balance(sum uint32, pool []Server) (string, error) {
	var healthyServers []string

	for _, server := range pool {
		if server.isHealthy {
			healthyServers = append(healthyServers, server.name)
		}
	}

	if len(healthyServers) == 0 {
		return "", errors.New("no healthy servers")
	}

	serverIndex := sum % uint32(len(healthyServers))
	return healthyServers[serverIndex], nil
}

func main() {
	flag.Parse()

	for i := range serversPool {
		i := i
		go func() {
			for range time.Tick(10 * time.Second) {
				server := serversPool[i].name
				isHealthy := health(server)
				serversPool[i].isHealthy = isHealthy
				log.Println(server, isHealthy)
			}
		}()
	}

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		sum, _ := hash(r.RemoteAddr)
		if *traceEnabled {
			log.Printf("Client's IP: %s, hashsum: %d", r.RemoteAddr, sum)
		}
		server, err := balance(sum, serversPool)
		if err != nil {
			log.Printf("return 503, no servers avaliable")
			rw.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		forward(server, rw, r)
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}
