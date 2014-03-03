package main

import (
  "fmt"
  "strings"
)

func SplitOnSlash(r rune) bool {
  return r == '/'
}

func main() {
  path := "/ls/foo/wombat/pouch"
  parts := strings.FieldsFunc(path, SplitOnSlash)
  fmt.Println(parts)
  for i := 0; i <= len(parts); i++ {
    fmt.Println(strings.Join(parts[0:i], "/"))
  }
}
