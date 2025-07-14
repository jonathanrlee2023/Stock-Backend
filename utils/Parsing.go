package utils

import (
	"fmt"
	"strconv"
	"time"
)

// Parses the full option id into each component
func ParseOptionString(s string) (OptionStreamRequest, error) {
	var req OptionStreamRequest

	// Split at underscore
	parts := []rune(s)
	underscoreIndex := -1
	for i, c := range parts {
		if c == '_' {
			underscoreIndex = i
			break
		}
	}

	if underscoreIndex == -1 || len(parts) < underscoreIndex+13 {
		return req, fmt.Errorf("invalid string format")
	}

	req.Symbol = string(parts[:underscoreIndex])

	req.Year = string(parts[underscoreIndex+1 : underscoreIndex+3])
	req.Month = string(parts[underscoreIndex+3 : underscoreIndex+5])
	req.Day = string(parts[underscoreIndex+5 : underscoreIndex+7])
	req.Type = string(parts[underscoreIndex+7])

	// Extract strike price
	priceStr := string(parts[underscoreIndex+8:])
	if len(priceStr) < 8 {
		return req, fmt.Errorf("invalid strike price length")
	}
	// convert to decimal
	priceInt, err := strconv.Atoi(priceStr)
	if err != nil {
		return req, fmt.Errorf("invalid price number: %v", err)
	}
	req.Price = fmt.Sprintf("%.2f", float64(priceInt)/1000.0)

	return req, nil
}

func ParseExpirationDate(req OptionStreamRequest) (time.Time, error) {
	day, err := strconv.Atoi(req.Day)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid day: %v", err)
	}
	month, err := strconv.Atoi(req.Month)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid month: %v", err)
	}
	yearStr := req.Year
	var year int
	if len(yearStr) == 2 {
		// Interpret YY as 2000+YY if YY < 50, else 1900+YY
		yy, err := strconv.Atoi(yearStr)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid year: %v", err)
		}
		if yy < 50 {
			year = 2000 + yy
		} else {
			year = 1900 + yy
		}
	} else if len(yearStr) == 4 {
		year, err = strconv.Atoi(yearStr)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid year: %v", err)
		}
	} else {
		return time.Time{}, fmt.Errorf("invalid year length: %s", yearStr)
	}

	return time.Date(year, time.Month(month), day, 23, 59, 59, 0, time.UTC), nil
}
