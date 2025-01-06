package utils

import (
	"fmt"
	"math"
	"sort"
	"time"

	"gonum.org/v1/gonum/stat"
)

func SlopeFunctions(result AlpacaOptionsResponse, fullKey string) map[time.Time]float64 {
	symbolData := result.Bars[fullKey]
	points := make(map[time.Time]float64)
	functions := make(map[time.Time][]float64)
	priceMap := make(map[time.Time]float64)
	var timeArr []time.Time

	// Populate timeArr and priceMap with prices and their according times
	for _, x := range symbolData {
		timeArr = append(timeArr, x.T)
		priceMap[x.T] = x.C
	}

	// Sort timeArr to ensure chronological order
	sort.Slice(timeArr, func(i, j int) bool {
		return timeArr[i].Before(timeArr[j])
	})

	// If less than two points, return nil as slopes cannot be calculated
	if len(timeArr) < 2 {
		return nil
	}

	// Calculate slopes
	for i := 0; i < len(timeArr)-1; i++ {
		y2 := priceMap[timeArr[i+1]] // Closing price at i+1
		y1 := priceMap[timeArr[i]]   // Closing price at i
		timeDiff := timeArr[i+1].Sub(timeArr[i]).Minutes()

		// Avoid division by zero
		if timeDiff == 0 {
			continue
		}

		// Calculate slope
		slope := (y2 - y1) / timeDiff
		functions[timeArr[i]] = []float64{slope, timeDiff}
	}

	// Generate points using slopes
	for i := 0; i < len(timeArr)-1; i++ {
		currentTime := timeArr[i]
		slope := functions[currentTime][0]
		duration := int(functions[currentTime][1])

		for j := 0; j <= duration; j += 5 {
			pointTime := currentTime.Add(time.Duration(j) * time.Minute)
			points[pointTime] = priceMap[currentTime] + (slope * float64(j))
		}
	}

	return points
}

func StandardDev(result StockResponse) float64 {
	var data []float64
	for _, price := range result.Results {
		data = append(data, price.C)
	}

	stdDev := stat.StdDev(data, nil)

	return stdDev
}

func CalculateHistoricalVolatility(prices StockResponse, tradingDaysPerYear float64) (float64, float64) {
	if prices.ResultsCount < 2 {
		return 0, 0
	}

	closePrice := make(map[int64]float64)

	var keys []int64

	for _, x := range prices.Results {
		keys = append(keys, x.T)
		closePrice[x.T] = x.C
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	// Calculate daily returns
	returns := make([]float64, prices.Count-1)
	for i := len(keys) / 2; i < len(keys); i++ {
		// Using log returns for more accurate volatility calculation
		returns[i-1] = math.Log(closePrice[keys[i]] / closePrice[keys[i-1]])
	}

	// Calculate mean of returns
	mean := 0.0
	for _, r := range returns {
		mean += r
	}
	mean /= float64(len(returns))

	// Calculate variance
	variance := 0.0
	for _, r := range returns {
		diff := r - mean
		variance += diff * diff
	}
	variance /= float64(len(returns) - 1)

	// Convert daily volatility to annualized volatility
	annualizedVol := math.Sqrt(variance * tradingDaysPerYear)

	return annualizedVol * 100, closePrice[keys[len(keys)-1]]
}

// Option represents the parameters of an option contract
type Option struct {
	S  float64 // Current stock price
	K  float64 // Strike price
	T  float64 // Time to expiration (in years)
	R  float64 // Risk-free rate
	P  float64 // Market price of option
	CP string  // Call or Put flag ("C" or "P")
}

// Standard normal cumulative distribution function
func normCDF(x float64) float64 {
	return 0.5 * (1 + math.Erf(x/math.Sqrt(2)))
}

// Black-Scholes option price calculation
func blackScholes(S, K, T, r, sigma float64, optType string) float64 {
	d1 := (math.Log(S/K) + (r+sigma*sigma/2)*T) / (sigma * math.Sqrt(T))
	d2 := d1 - sigma*math.Sqrt(T)

	if optType == "C" {
		return S*normCDF(d1) - K*math.Exp(-r*T)*normCDF(d2)
	}
	// Put option
	return K*math.Exp(-r*T)*normCDF(-d2) - S*normCDF(-d1)
}

// Vega calculation for the Black-Scholes model
func blackScholesVega(S, K, T, r, sigma float64) float64 {
	d1 := (math.Log(S/K) + (r+sigma*sigma/2)*T) / (sigma * math.Sqrt(T))
	return S * math.Sqrt(T) * math.Exp(-d1*d1/2) / math.Sqrt(2*math.Pi)
}

// Calculate implied volatility using Newton-Raphson method
func impliedVolatility(opt Option, initVol float64) (float64, error) {
	const (
		maxIter = 100
		epsilon = 1e-5
	)

	sigma := initVol
	for i := 0; i < maxIter; i++ {
		price := blackScholes(opt.S, opt.K, opt.T, opt.R, sigma, opt.CP)
		diff := price - opt.P

		// Check if we've reached desired accuracy
		if math.Abs(diff) < epsilon {
			return sigma, nil
		}

		vega := blackScholesVega(opt.S, opt.K, opt.T, opt.R, sigma)
		// Avoid division by zero
		if math.Abs(vega) < 1e-10 {
			return 0, fmt.Errorf("vega too close to zero")
		}

		// Update volatility estimate
		sigma = sigma - diff/vega

		// Check for invalid volatility
		if sigma <= 0 {
			sigma = 0.0001 // Reset to small positive value
		}
	}

	return 0, fmt.Errorf("failed to converge after %d iterations", maxIter)
}

func RoundToNearestFive(value float64) string {
	rounded := math.Round(value/5) * 5

	// Convert the rounded result to a string with 3 digits, padded with zeros if necessary
	return fmt.Sprintf("%03d", int(rounded))
}
