package main

import (
	"fmt"
	"math/rand"
	"time"
)

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
	keepaliveDuration := 500 * time.Millisecond
	timer := time.NewTimer(keepaliveDuration)
	alive := true
	for alive == true {
		select {
		case <-time.After(time.Duration(400+rand.Intn(1000)) * time.Millisecond):
			fmt.Println("Died...")
			alive = false
		case <-timer.C:
			fmt.Println("Survived")
			timer.Reset(keepaliveDuration)
		}
	}
}
