package SkipList

import "fmt"

func mainooo() {
	fmt.Println("start")

	for i:=1; i < 10; i++ {
		defer fmt.Println(i)
	}

	fmt.Println("end")
}
