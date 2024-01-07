package main

import (
	"encoding/csv"
	"fmt"
	"os"
)

func SaveToCsv(filepath string, list [][]string) error {
	file, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("can not create file: %w", err)
	}
	defer file.Close()

	file.WriteString("\xEF\xBB\xBF")

	writer := csv.NewWriter(file)
	writer.Comma = ','
	writer.UseCRLF = true

	writer.WriteAll(list)
	writer.Flush()

	if err := writer.Error(); err != nil {
		return fmt.Errorf("error writing CSV file: %w", err)
	}
	return nil
}
