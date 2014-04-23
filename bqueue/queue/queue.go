package queue

import(
)


type Node struct {
    value string
    next *Node
}

type Queue struct {
    head *Node
    tail *Node
    count int
}


func NewQueue() *Queue {
    q := &Queue{}
    return q
}

func (q *Queue) Len(_ int, reply *int) error {
    *reply = q.count

    return nil
}

func (q *Queue) Push(data string, _ int) error {
    n := &Node {data, nil}

    if q.tail == nil {
        q.tail = n
        q.head = n
    } else {
        q.tail.next = n
        q.tail = n
    }
    q.count++

    return nil
}

func (q *Queue) Pop(_ int, reply *string) error {
    if q.head == nil {
        return nil
    }

    n := q.head
    q.head = n.next

    if q.head == nil {
        q.tail = nil
    }
    q.count--

    *reply = n.value

    return nil
}

func (q *Queue) Peek(_ int, reply *string) error {
    n := q.head

    if n == nil || len(n.value) == 0 {
        return nil
    }

    *reply = n.value

    return nil
}


