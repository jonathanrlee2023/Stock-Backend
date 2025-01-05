package main

import (
	"Go-API/utils"
	"fmt"
	"log"
	"net/http"
)

func main() {
	// Empty cache folder everytime API is turned on
	folderPath := "./StockDataCache"
	err := utils.DeleteContents(folderPath)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Println("Folder contents deleted successfully!")
	}
	// start api
	mux := http.NewServeMux()
	mux.HandleFunc("/options", utils.OptionsHandler)
	mux.HandleFunc("/earnings", utils.EarningsHandler)
	mux.HandleFunc("/stock", utils.StockHandler)

	handler := utils.CorsMiddleware(mux)

	fmt.Println("Server is running on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", handler))
}
