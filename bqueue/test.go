package main

import (
    "./queue"
    "fmt"
)



func main() {
    var _e int //discard type
    var reply int
    var value string

    q := queue.NewQueue()
    q.Push("cat", _e)

    q.Len(_e, &reply)
    fmt.Println(reply)

    q.Push("meeeeeow", _e)

    q.Len(_e, &reply)
    fmt.Println(reply)

    q.Pop(_e, &value)
    fmt.Println(value)
}
