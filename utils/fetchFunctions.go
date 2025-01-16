package utils

import (
	"io"
	"net/http"
)

// Makes an API call to the Alpaca Api with the api key and secret key
func fetchAlpacaAPIWithHeaders(url, keyID, secretKey string) ([]byte, error) {
	req, _ := http.NewRequest("GET", url, nil)

	req.Header.Add("accept", "application/json")
	req.Header.Add("APCA-API-KEY-ID", keyID)
	req.Header.Add("APCA-API-SECRET-KEY", secretKey)

	res, _ := http.DefaultClient.Do(req)

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)

	return body, nil
}

// default api calls with no special headers
func fetchDefaultAPI(url string) ([]byte, error) {
	req, _ := http.NewRequest("GET", url, nil)

	req.Header.Add("Content-Type", "application/json")

	res, _ := http.DefaultClient.Do(req)

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)

	return body, nil
}
