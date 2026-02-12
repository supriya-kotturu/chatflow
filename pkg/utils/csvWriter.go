// Package utils provides shared utilities for the ChatFlow application.
package utils

import (
	"encoding/csv"
	"log"
	"os"
	"path/filepath"
	"sync"

	"supriyakotturu.github.com/chatflow/pkg/models"
)

// CSVWriter is a thread-safe wrapper around csv.Writer for recording metrics.
type CSVWriter struct {
	FileName string
	Writer   *csv.Writer
	Fd       *os.File
	mu       *sync.Mutex
}

// NewCSVWriter creates or truncates the given file and returns a CSVWriter.
func NewCSVWriter(fileName string) (*CSVWriter, error) {
	if dir := filepath.Dir(fileName); dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Printf("Error creating directory %s: %+v", dir, err)
			return nil, err
		}
	}
	fd, err := os.Create(fileName)
	if err != nil {
		log.Printf("Error creating the file %s: %+v", fileName, err)
		return nil, err
	}
	writer := csv.NewWriter(fd)

	return &CSVWriter{
		FileName: fileName,
		Writer:   writer,
		Fd:       fd,
		mu:       &sync.Mutex{},
	}, nil
}

// WriteHeader writes the CSV column headers.
func (cw *CSVWriter) WriteHeader() {
	header := []string{"Timestamp", "MessageType", "Latency", "StatusCode", "RoomId"}
	if err := cw.Writer.Write(header); err != nil {
		log.Printf("Error writing header to CSV: %+v", err)
	}
}

// Write appends a single metric row to the CSV file.
func (cw *CSVWriter) Write(metric models.Metric) {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	if err := cw.Writer.Write(metric.String()); err != nil {
		log.Printf("Error writing record to CSV: %+v", err)
	}
}

// WriteAll writes multiple metric rows to the CSV file in one call.
func (cw *CSVWriter) WriteAll(metrics []models.Metric) {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	records := make([][]string, 0)
	for _, m := range metrics {
		records = append(records, m.String())
	}

	cw.Writer.WriteAll(records)
}

// Flush writes any buffered data to the underlying file.
func (cw *CSVWriter) Flush() {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	cw.Writer.Flush()
	if err := cw.Writer.Error(); err != nil {
		log.Printf("Error flushing CSV writer: %+v", err)
	}
}
