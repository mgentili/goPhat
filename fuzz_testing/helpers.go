package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"github.com/mgentili/goPhat/level_log"
	"math/rand"
	"os"
	"strconv"
)

func (t *TestMaster) Debug(level int, format string, args ...interface{}) {
	t.log.Printf(level, format, args...)
}

func (t *TestMaster) Hash(stuff interface{}) string {
	var state bytes.Buffer

	// Encode the log state
	enc := gob.NewEncoder(&state)
	err := enc.Encode(stuff)
	if err != nil {
		t.Debug(DEBUG, "Cannot hash it!")
	}
	// Hash the database state
	hash := sha256.New()
	hash.Write(state.Bytes())
	md := hash.Sum(nil)
	return hex.EncodeToString(md)
}

// readLines reads a whole file into memory
// and returns a slice of its lines (assuming one int per line).
func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func generateRandomString() string {
	return strconv.Itoa(rand.Int())
}

func (t *TestMaster) SetupLog() {
	levelsToLog := []int{DEBUG}
	t.log = level_log.NewLL(os.Stdout, "TM: ")
	t.log.SetLevelsToLog(levelsToLog)
}

// Go doesn't have an atexit
// https://groups.google.com/d/msg/golang-nuts/qBQ0bK2zvQA/vmOu9uhkYH0J
func (t *TestMaster) DieClean(v ...interface{}) {
	t.log.Fatal(DEBUG, v)
}
