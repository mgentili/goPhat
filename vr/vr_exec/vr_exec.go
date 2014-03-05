package main

import (
    "flag"
    "goPhat/vr"
)

func main() {
    indP := flag.Uint("r", 0, "replica num")
    flag.Parse()

    ind := *indP

    var config = []string{"127.0.0.1:9000", "127.0.0.1:9001", "127.0.0.1:9002"}

    vr.RunAsReplica(ind, config)
    <-make(chan int)
}