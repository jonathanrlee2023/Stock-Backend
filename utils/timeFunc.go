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

func MostRecentWeekday(t time.Time) time.Time {
	if t.Weekday() >= time.Monday && t.Weekday() <= time.Friday {
		return t
	}

	daysToSubtract := 0
	switch t.Weekday() {
	case time.Saturday:
		daysToSubtract = 1
	case time.Sunday:
		daysToSubtract = 2
	}

	return t.AddDate(0, 0, -daysToSubtract)
}

func FindCommonTimes(slice1, slice2 []time.Time) (earliest, latest *time.Time) {
	i, j := 0, 0

	for i < len(slice1) && j < len(slice2) {
		if slice1[i].Equal(slice2[j]) {
			// Record the earliest common time if not already set
			if earliest == nil {
				earliest = &slice1[i]
			}
			// Update the latest common time
			latest = &slice1[i]
			i++
			j++
		} else if slice1[i].Before(slice2[j]) {
			i++
		} else {
			j++
		}
	}

	return earliest, latest
}
