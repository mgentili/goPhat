package main

import (
	"fmt"
	"github.com/mgentili/goPhat/phatclient"
	"github.com/mgentili/goPhat/phatRPC"
)

const BASE = 9000

func main() {
	var serverLocations []string
	for i := 0; i < 5; i = i + 1 {
		phatRPC.StartServer(i+BASE, 1)
		serverLocations = append(serverLocations, fmt.Sprintf("localhost:%d", i+BASE))
	}
	cli, err := phatclient.NewClient(serverLocations, 3)
	if err != nil {
		fmt.Printf(err.Error())
	}

	_, err = cli.GetData("/dev/null")
	fmt.Println("GOT", err.Error())
	if err != nil {
		fmt.Printf("!%s\n", err.Error())
	} else {
		panic("Expected error from GetData")
	}
	
	_, err = cli.Create("/dev/null", "empty")
	if err != nil {
		panic(fmt.Sprintf("Expected no error from Create, got %s", err))
	}

	n, err := cli.GetData("/dev/null")
	if err != nil {
		panic("Expected no error from GetData")
	}
	fmt.Printf("Expected %s, got %s\n", "empty", n.Value)
	
}
