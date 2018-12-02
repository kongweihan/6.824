package main

import (
	"os"
	"sort"
	"strings"
	"unicode"
)
import "fmt"
import "mapreduce"

// The mapping function is called once for each piece of the input.
// In this framework, the key is the name of the file that is being processed,
// and the value is the file's contents. The return value should be a slice of
// key/value pairs, each represented by a mapreduce.KeyValue.
func mapF(document string, value string) (res []mapreduce.KeyValue) {
	// Your code here (Part V).
	words := strings.FieldsFunc(value, func(r rune) bool {
		return !unicode.IsLetter(r)
	})
	var keyValues []mapreduce.KeyValue
	for _, w := range words {
		keyValues = append(keyValues, mapreduce.KeyValue{Key: w, Value: document})
	}
	return keyValues
}

// The reduce function is called once for each key generated by Map, with a
// list of that key's string value (merged across all inputs). The return value
// should be a single output value for that key.
func reduceF(key string, values []string) string {
	// Your code here (Part V).
	set := make(map[string]bool)
	for _, word := range values {
		set[word] = true
	}
	var list []string
	for k := range set {
		list = append(list, k)
	}
	sort.Strings(list)
	output := fmt.Sprintf("%v %v", len(list), list[0])
	for i := 1; i < len(list); i++ {
		output += "," + list[i]
	}
	return output
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("iiseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("iiseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100, nil)
	}
}
