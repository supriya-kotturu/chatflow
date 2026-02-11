package utils

import (
	"encoding/csv"
	"log"
	"os"
	"sync"

	"supriyakotturu.github.com/chatflow/pkg/models"
)

type CSVWriter struct {
	FileName string
	Writer   *csv.Writer
	Fd       *os.File
	mu       *sync.Mutex
}

func NewCSVWriter(fileName string) (*CSVWriter, error) {
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

func (cw *CSVWriter) WriteHeader() {
	header := []string{"Timestamp", "MessageType", "Latency", "StatusCode", "RoomId"}
	if err := cw.Writer.Write(header); err != nil {
		log.Printf("Error writing header to CSV: %+v", err)
	}
}

func (cw *CSVWriter) Write(metric models.Metric) {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	if err := cw.Writer.Write(metric.String()); err != nil {
		log.Printf("Error writing record to CSV: %+v", err)
	}
}

func (cw *CSVWriter) WriteAll(metrics []models.Metric) {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	records := make([][]string, 0)
	for _, m := range metrics {
		records = append(records, m.String())
	}

	cw.Writer.WriteAll(records)
}

func (cw *CSVWriter) Flush() {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	cw.Writer.Flush()
	if err := cw.Writer.Error(); err != nil {
		log.Printf("Error flushing CSV writer: %+v", err)
	}
}
