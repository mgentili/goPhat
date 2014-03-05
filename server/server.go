package main

import (
	"fmt"
	"github.com/mgentili/goPhat"
)

const BASE = 9000

func main() {
	var serverLocations []string
	for i := 0; i < 5; i = i + 1 {
		gophat.StartServer(i+BASE, 1)
		serverLocations = append(serverLocations, fmt.Sprintf("localhost:%d", i+BASE))
	}
	cli, err := gophat.NewClient(serverLocations, 3)
	if err != nil {
		fmt.Printf(err.Error())
	}

	_, err = cli.GetData("/dev/null")
	if err != nil {
		fmt.Printf(err.Error())
	}
}
