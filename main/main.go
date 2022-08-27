package main

import "KeyValueEngine/Core/DataBase"

func main() {
	db := DataBase.InitDataBase()
	db.Put("1", "Srdjan")
	db.Put("2", "Milos")
}
