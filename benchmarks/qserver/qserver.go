package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/mgentili/goPhat/queueRPC"
	"github.com/mgentili/goPhat/vr"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

func localIP() (net.IP, error) {
	// https://groups.google.com/forum/#!topic/golang-nuts/WXKmB1MI-6g
	tt, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, t := range tt {
		aa, err := t.Addrs()
		if err != nil {
			return nil, err
		}
		for _, a := range aa {
			ipnet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			v4 := ipnet.IP.To4()
			if v4 == nil || v4[0] == 127 { // loopback address
				continue
			}
			return v4, nil
		}
	}
	return nil, errors.New("cannot find local IP address")
}

func main() {
	// Set and read the command line flags
	rawServerPaths := flag.String("servers", "", "Path to servers, space delimited")
	initPosition := flag.Int("pos", -1, "Position in server list (if blank, attempts to use IP to guess)")
	local := flag.Bool("local", false, "States the test is running on a single machine")
	flag.Parse()
	if *local {
		*rawServerPaths = "127.0.0.1:9000 127.0.0.1:9001 127.0.0.1:9002"
	}
	serverPaths := strings.Fields(*rawServerPaths)

	ip, _ := localIP()
	log.Println('My IP address is', ip)

	// Work out which address this server should use
	// If we're not local, use our IP address
	// If we're local, use the position we've been given
	position := *initPosition
	if !*local {
		for i := 0; i < len(serverPaths); i = i + 1 {
			if position == -1 && serverPaths[i] == ip.String() {
				position = i
			}
		}
	}
	if position == -1 {
		log.Fatal("Couldn't find my position in the servers array")
	}

	// Start VR and the Queue RPC server
	fmt.Println("Starting VR server at " + serverPaths[position] + "...")
	newReplica := vr.RunAsReplica(uint(position), serverPaths)
	port := 1337
	if *local {
		port += position
	}
	rpcServerPath := ip.String() + ":" + strconv.FormatInt(int64(port), 10)
	fmt.Println("Starting RPC server at " + rpcServerPath + "...")
	queueRPC.StartServer(rpcServerPath, newReplica)

	// Survive indefinitely
	t := time.NewTicker(1 * time.Minute)
	for _ = range t.C {
	}
}
