package helpers

import "strings"

// Helper function to check if a string exists in a slice
func ContainsItem(slice []string, item string) bool {
	for _, val := range slice {
		if val == item {
			return true
		}
	}
	return false
}

// isValuePresentInTheList is a util method which checks whether a particular string
// is present in a given list or not
func IsValuePresentInTheList(strArr []string, str string) bool {
	for _, s := range strArr {
		if strings.Contains(s, str) {
			return true
		}
	}
	return false
}
