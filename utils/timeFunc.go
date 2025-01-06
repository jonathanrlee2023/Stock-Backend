package utils

import (
	"fmt"
	"time"
)

func NextWeekFriday() (int, string, string) {
	// Get the current time
	now := time.Now()

	// Find the offset to the next Friday
	daysUntilFriday := int(time.Friday) - int(now.Weekday())
	if daysUntilFriday <= 0 {
		daysUntilFriday += 7
	}

	// Add the offset to move to this week's Friday
	thisWeekFriday := now.AddDate(0, 0, daysUntilFriday)

	// Add 7 days to find the Friday of next week
	nextWeekFriday := thisWeekFriday.AddDate(0, 0, 7)

	// Return year as an integer, and month/day as zero-padded strings
	year := nextWeekFriday.Year()
	month := fmt.Sprintf("%02d", int(nextWeekFriday.Month()))
	day := fmt.Sprintf("%02d", nextWeekFriday.Day())

	return year, month, day
}
