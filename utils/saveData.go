package utils

import (
	"fmt"
	"os"
	"path/filepath"
)

func CreateFile(name string) string {
	// Names the file ticker.txt
	fileName := fmt.Sprintf("%s.json", name)

	// Creates file with specific name
	file, err := os.Create(fileName)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return ""
	}

	// Closes file and returns file address
	file.Close()
	return fileName
}

func WriteDataToFile(FileName string, Data string) {
	// Opens file to append with write permissions
	file, err := os.OpenFile(FileName, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}

	defer file.Close()

	// Writes data on file
	_, err = file.WriteString(Data)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return
	}
}

func ReadFile(FileName string) string {
	// Reads from file
	data, err := os.ReadFile(FileName)
	if err != nil {
		fmt.Println("Error reading file:", err)
		return ""
	}

	// Convert the data to a string and return it
	return string(data)
}

// Checks if the file with a certain name exists
func FileExists(fileName string) bool {
	_, err := os.Stat(fileName)
	if os.IsNotExist(err) {
		return false
	}
	return err == nil
}

func DeleteContents(folderPath string) error {
	// Read the directory contents
	dirEntries, err := os.ReadDir(folderPath)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	// Iterate through each entry and delete it
	for _, entry := range dirEntries {
		entryPath := filepath.Join(folderPath, entry.Name())
		err := os.RemoveAll(entryPath)
		if err != nil {
			return fmt.Errorf("failed to delete %s: %w", entryPath, err)
		}
	}

	return nil
}
