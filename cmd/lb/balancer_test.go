package main

import (
	"github.com/stretchr/testify/require"
	"testing"
)

type BalancerTestCase struct {
	Hash uint32
	Pool []Server
	Server string
	Error bool
}

func (c BalancerTestCase) test(t *testing.T) {
	server, err := balance(c.Hash, c.Pool)
	require.Equal(t, c.Server, server)
	require.Equal(t, c.Error, err != nil)
}

func TestBalancer(t *testing.T) {
	for _, testCase := range []BalancerTestCase{
		{
			Hash:   0,
			Pool:   []Server{
				{
					name:      "server1",
					isHealthy: true,
				},
				{
					name:      "server2",
					isHealthy: true,
				},
				{
					name:      "server3",
					isHealthy: true,
				},
			},
			Server: "server1",
			Error:  false,
		},
		{
			Hash:   1,
			Pool:   []Server{
				{
					name:      "server1",
					isHealthy: true,
				},
				{
					name:      "server2",
					isHealthy: true,
				},
				{
					name:      "server3",
					isHealthy: true,
				},
			},
			Server: "server2",
			Error:  false,
		},
		{
			Hash:   2,
			Pool:   []Server{
				{
					name:      "server1",
					isHealthy: true,
				},
				{
					name:      "server2",
					isHealthy: true,
				},
				{
					name:      "server3",
					isHealthy: true,
				},
			},
			Server: "server3",
			Error:  false,
		},
		{
			Hash:   3,
			Pool:   []Server{
				{
					name:      "server1",
					isHealthy: true,
				},
				{
					name:      "server2",
					isHealthy: true,
				},
				{
					name:      "server3",
					isHealthy: true,
				},
			},
			Server: "server1",
			Error:  false,
		},
		{
			Hash:   0,
			Pool:   []Server{
				{
					name:      "server1",
					isHealthy: true,
				},
				{
					name:      "server2",
					isHealthy: true,
				},
				{
					name:      "server3",
					isHealthy: false,
				},
			},
			Server: "server1",
			Error:  false,
		},
		{
			Hash:   1,
			Pool:   []Server{
				{
					name:      "server1",
					isHealthy: true,
				},
				{
					name:      "server2",
					isHealthy: true,
				},
				{
					name:      "server3",
					isHealthy: false,
				},
			},
			Server: "server2",
			Error:  false,
		},
		{
			Hash:   2,
			Pool:   []Server{
				{
					name:      "server1",
					isHealthy: true,
				},
				{
					name:      "server2",
					isHealthy: true,
				},
				{
					name:      "server3",
					isHealthy: false,
				},
			},
			Server: "server1",
			Error:  false,
		},
		{
			Hash:   3,
			Pool:   []Server{
				{
					name:      "server1",
					isHealthy: true,
				},
				{
					name:      "server2",
					isHealthy: true,
				},
				{
					name:      "server3",
					isHealthy: false,
				},
			},
			Server: "server2",
			Error:  false,
		},
		{
			Hash:   0,
			Pool:   []Server{
				{
					name:      "server1",
					isHealthy: true,
				},
				{
					name:      "server2",
					isHealthy: false,
				},
				{
					name:      "server3",
					isHealthy: false,
				},
			},
			Server: "server1",
			Error:  false,
		},
		{
			Hash:   1,
			Pool:   []Server{
				{
					name:      "server1",
					isHealthy: true,
				},
				{
					name:      "server2",
					isHealthy: false,
				},
				{
					name:      "server3",
					isHealthy: false,
				},
			},
			Server: "server1",
			Error:  false,
		},
		{
			Hash:   2,
			Pool:   []Server{
				{
					name:      "server1",
					isHealthy: true,
				},
				{
					name:      "server2",
					isHealthy: false,
				},
				{
					name:      "server3",
					isHealthy: false,
				},
			},
			Server: "server1",
			Error:  false,
		},
		{
			Hash:   0,
			Pool:   []Server{
				{
					name:      "server1",
					isHealthy: true,
				},
				{
					name:      "server2",
					isHealthy: true,
				},
				{
					name:      "server3",
					isHealthy: false,
				},
			},
			Server: "server1",
			Error:  false,
		},
		{
			Hash:   1,
			Pool:   []Server{
				{
					name:      "server1",
					isHealthy: true,
				},
				{
					name:      "server2",
					isHealthy: true,
				},
				{
					name:      "server3",
					isHealthy: false,
				},
			},
			Server: "server2",
			Error:  false,
		},
		{
			Hash:   2,
			Pool:   []Server{
				{
					name:      "server1",
					isHealthy: true,
				},
				{
					name:      "server2",
					isHealthy: true,
				},
				{
					name:      "server3",
					isHealthy: false,
				},
			},
			Server: "server1",
			Error:  false,
		},
		{
			Hash:   0,
			Pool:   []Server{
				{
					name:      "server1",
					isHealthy: false,
				},
				{
					name:      "server2",
					isHealthy: false,
				},
				{
					name:      "server3",
					isHealthy: false,
				},
			},
			Server: "",
			Error:  true,
		},
		{
			Hash: 0,
			Pool:   []Server{},
			Server: "",
			Error:  true,
		},
		{
			Hash: 0,
			Pool:   nil,
			Server: "",
			Error:  true,
		},
	} {
		testCase.test(t)
	}
}

type HashTestCase struct {
	Ip string
	Error bool
	Hash uint32
}

func (c HashTestCase) test(t *testing.T) {
	sum, err := hash(c.Ip)
	require.Equal(t, c.Hash, sum)
	require.Equal(t, c.Error, err != nil)
}

func TestHash(t *testing.T) {
	for _, testCase := range []HashTestCase{
		{
			Ip:    "0.0.0.1",
			Error: false,
			Hash:  0x0,
		},
		{
			Ip:    "10.0.0.5",
			Error: false,
			Hash:  0x500000A,
		},
		{
			Ip:    "10.0.0.5:8080",
			Error: false,
			Hash:  0x500000A,
		},
		{
			Ip:    "255.255.255.255",
			Error: false,
			Hash:  0xFFFFFFFF,
		},
		{
			Ip:    "0000",
			Error: true,
			Hash:  0,
		},
		{
			Ip:    "0.100.00",
			Error: true,
			Hash:  0,
		},
		{
			Ip:    "256.256.256.256",
			Error: true,
			Hash:  0,
		},
	} {
		testCase.test(t)
	}
}
