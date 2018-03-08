package main

import "fmt"

func main() {
	size, err := Du(".")
	if err != nil {
		panic(err)
	}
	fmt.Printf("size %d\n", size)
	entries, err := DuChilds(".")
	if err != nil {
		panic(err)
	}
	for _, entry := range entries {
		fmt.Printf("%s\t\t\t%d\n", entry.Name, entry.Size)
	}
}
