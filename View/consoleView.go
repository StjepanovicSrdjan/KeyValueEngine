package View

import (
	"KeyValueEngine/Core/DataBase"
	"KeyValueEngine/Core/Structures/CountMinSketch"
	"KeyValueEngine/Core/Structures/HyperLogLog"
	"fmt"
)

const mainMenu = "\n1. Put" +
	"\n2. Get" +
	"\n3. Delete" +
	"\n4. HyperLogLog options" +
	"\n5. Count min sketch options" +
	"\n0. Exit"

const hllMenu1 = "\n1. Create Hyper log log" +
	"\n2. Delete hll" +
	"\n3. Get hll" +
	"\n0. Back"

const cmsMenu1 = "\n1. Create Count min sketch" +
	"\n2. Delete cms" +
	"\n3. Get cms" +
	"\n0. Back"

const hllMenu2 = "\n1. Add" +
	"\n 2.Get estimated value" +
	"0. Back"

const cmsMenu2 = "\n1. Add" +
	"\n2. Get frequency" +
	"\n0. Back"


func Console() {
	db := DataBase.InitDataBase()
	var input string
	var key string
	var value string
	for {
		fmt.Println(mainMenu)
		fmt.Scan(&input)
		if input == "1" {
			fmt.Println("Input key: ")
			fmt.Scan(&key)
			fmt.Println("Input value: ")
			fmt.Scan(&value)
			data := []byte(value)
			db.Put(key, data)
			continue
		}else if input == "2" {
			fmt.Println("Input key: ")
			fmt.Scan(&key)
			found, valueByte := db.Get(key)
			if found {
				fmt.Println("Value: " + string(valueByte))
			}else{
				fmt.Println("Value not found!")
			}
			continue
		}else if input == "3"{
			fmt.Println("Input key: ")
			fmt.Scan(&key)
			succ := db.Delete(key)
			if succ {
				fmt.Println("You successfully deleted value with entered key.")
			}else{
				fmt.Println("Key not found.")
			}
			continue
		}else if input == "4" {
			hllOptions(db)
			continue
		}else if input == "5" {
			cmsOptions(db)
			continue
		} else if input == "0" {
			break
		}else{
			fmt.Println("Input error. Try again!")
			continue
		}
	}
}

func hllOptions(db *DataBase.DataBase) {
	var input string
	var key string
	var item string
	for {
		fmt.Println(hllMenu1)
		fmt.Scan(&input)
		if input == "1" {
			hll := HyperLogLog.InitHLL(4)
			fmt.Println("Input key: ")
			fmt.Scan(&key)
			db.PutHll(key, *hll)
			continue
		}else if input == "2" {
			fmt.Println("Input key: ")
			fmt.Scan(&key)
			succ := db.Delete(key)
			if succ {
				fmt.Println("You successfully deleted value with entered key.")
			}else{
				fmt.Println("Key not found.")
			}
			continue
		}else if input == "3" {
			fmt.Println("Input key: ")
			fmt.Scan(&key)
			found, hllByte := db.Get(key)
			if !found {
				fmt.Println("Key not found.")
			}else{
				hll := HyperLogLog.HLL{}
				hll.Decode(hllByte)
				for {
					fmt.Println(hllMenu2)
					fmt.Scan(&input)
					if input == "1" {
						fmt.Println("Input item: ")
						fmt.Scan(&item)
						hll.Add(item)
						continue
					}else if input == "2" {
						fmt.Println(hll.Estimate())
						continue
					}else if input == "0" {
						break
					}else {
						fmt.Println("Input error. Try again!")
						continue
					}
				}
				fmt.Println("Input key: ")
				fmt.Scan(&key)
				db.PutHll(key, hll)
			}
			continue
		}else if input == "0" {
			break
		}else {
			fmt.Println("Input error. Try again!")
			continue
		}
	}
}

func cmsOptions(db *DataBase.DataBase) {
	var input string
	var key string
	var item string
	for {
		fmt.Println(cmsMenu1)
		fmt.Scan(&input)
		if input == "1" {
			cms := CountMinSketch.InitCMS(0.01, 0.01)
			fmt.Println("Input key: ")
			fmt.Scan(&key)
			db.PutCms(key, *cms)
			continue
		}else if input == "2" {
			fmt.Println("Input key: ")
			fmt.Scan(&key)
			succ := db.Delete(key)
			if succ {
				fmt.Println("You successfully deleted value with entered key.")
			}else{
				fmt.Println("Key not found.")
			}
			continue
		}else if input == "3" {
			fmt.Println("Input key: ")
			fmt.Scan(&key)
			found, cmsByte := db.Get(key)
			if !found {
				fmt.Println("Key not found.")
			}else{
				cms := CountMinSketch.CountMinSketch{}
				cms.Decode(cmsByte)
				for {
					fmt.Println(cmsMenu2)
					fmt.Scan(&input)
					if input == "1" {
						fmt.Println("Input item: ")
						fmt.Scan(&item)
						cms.Add(item)
						continue
					}else if input == "2" {
						fmt.Println("Input item: ")
						fmt.Scan(&item)
						fmt.Println(cms.GetFrequency(item))
						continue
					}else if input == "0" {
						break
					}else {
						fmt.Println("Input error. Try again!")
						continue
					}
				}
				fmt.Println("Input key: ")
				fmt.Scan(&key)
				db.PutCms(key, cms)
			}
			continue
		}else if input == "0" {
			break
		}else {
			fmt.Println("Input error. Try again!")
			continue
		}
	}
}
