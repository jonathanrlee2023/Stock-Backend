package main

import (
	"Go-API/utils"
	"fmt"
	"log"
	"net/http"
)

func main() {
	// Empty cache folder everytime API is turned on
	folderPath := []string{"./StockDataCache", "./TodayStockDataCache"}
	for _, value := range folderPath {
		err := utils.DeleteContents(value)
		if err != nil {
			fmt.Println("Error:", err)
		} else {
			fmt.Println("Folder contents deleted successfully!")
		}
	}
	// start api
	mux := http.NewServeMux()
	mux.HandleFunc("/options", utils.OptionsHandler)
	mux.HandleFunc("/earningsCalender", utils.EarningsCalenderHandler)
	mux.HandleFunc("/stock", utils.StockHandler)
	mux.HandleFunc("/earningsVolatility", utils.EarningsVolatilityHandler)
	mux.HandleFunc("/todayStock", utils.TodayStockHandler)
	mux.HandleFunc("/economicData", utils.EconomicDataHandler)

	handler := utils.CorsMiddleware(mux)

	fmt.Println("Server is running on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", handler))
}
