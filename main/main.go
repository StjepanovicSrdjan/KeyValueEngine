package main

import "KeyValueEngine/Core/DataBase"

func main() {
	db := DataBase.InitDataBase()
	db.Put("1", "Srdjan")
	db.Put("2", "M")
	db.Put("3", "Mi")
	db.Put("4", "Mil")
	db.Put("5", "Milo")
	db.Put("6", "Milos")
	db.Put("7", "Miloss")
	db.Put("8", "Milosss")

}
