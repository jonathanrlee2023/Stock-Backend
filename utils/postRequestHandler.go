package utils

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

type PostData struct {
	FileNames []string `json:"filenames"`
}

type OptionData struct {
	BidPrice  float64 `json:"Bid Price"`
	AskPrice  float64 `json:"Ask Price"`
	LastPrice float64 `json:"Last Price"`
	HighPrice float64 `json:"High Price"`
	Delta     float64 `json:"Delta"`
	Gamma     float64 `json:"Gamma"`
	Theta     float64 `json:"Theta"`
	Vega      float64 `json:"Vega"`
}

type OptionsMap struct {
	Data   map[string]OptionData `json:"Data"`
	Latest string                `json:"Latest"`
}

func PostHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var fileNameData PostData

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	err = json.Unmarshal(body, &fileNameData)
	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	for _, fileName := range fileNameData.FileNames {
		file, _ := os.ReadFile(fileName)

		var options OptionsMap
		if err := json.Unmarshal(file, &options); err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Println(options.Data[options.Latest].LastPrice)
	}

	fmt.Fprintf(w, "Data has been read")
}
