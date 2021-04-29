package integration

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
	"time"
)

const baseAddress = "http://balancer:8091"

var client = http.Client{
	Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
	var servName string
	for i := 0; i < 10; i++ {
		url := fmt.Sprintf("%s/api/v1/some-data", baseAddress)
		t.Log(fmt.Sprintf("Sending request to %s", url))
		resp, err := client.Get(url)
		if err != nil {
			t.Error(err)
		}

		if resp.StatusCode != http.StatusOK {
			t.Error(fmt.Sprintf("Response code: %d", resp.StatusCode))
		}

		t.Logf("response from [%s]", resp.Header.Get("lb-from"))
		if i == 0 {
			servName = resp.Header.Get("lb-from")
		} else {
			require.Equal(t, servName, resp.Header.Get("lb-from"))
		}
	}
}

func BenchmarkBalancer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			b.Error(err)
		}
		if resp.StatusCode != http.StatusOK {
			b.Error(fmt.Sprintf("Response code: %d", resp.StatusCode))
		}
	}
}
