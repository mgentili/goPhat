package main

import (
    "flag"
    "goPhat/vr"
)

var config = []string{"127.0.0.1:9000", "127.0.0.1:9001", "127.0.0.1:9002"}

const N = 3


func main() {
    oneProcP := flag.Bool("1", false, "Run VR in 1 process")
    indP := flag.Uint("r", 0, "replica num")
    flag.Parse()

    if *oneProcP {
        for ind := N-1; ind >= 0; ind-- {
            go vr.RunAsReplica(uint(ind), config)
        }
    } else {
        ind := *indP
        vr.RunAsReplica(ind, config)
    }
    <-make(chan int)
}