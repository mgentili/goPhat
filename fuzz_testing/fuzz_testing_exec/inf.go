package main

import "fmt"
import "time"

func main() {
	for {
		fmt.Println("Boom")
		time.Sleep(time.Second)
	}
}
