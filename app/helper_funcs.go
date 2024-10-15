package main

import "fmt"

func printByteSliceAsGoCode(data []byte) {
	fmt.Print("[]byte{")
	for i, b := range data {
		if i > 0 {
			fmt.Print(", ")
		}
		fmt.Printf("0x%02x", b)
	}
	fmt.Println("}")
}
