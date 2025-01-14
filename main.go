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
	mux.HandleFunc("/impliedVolatility", utils.OptionVolatilityHandler)

	handler := CorsMiddleware(mux)

	fmt.Println("Server is running on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", handler))
}

func CorsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "*")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}
