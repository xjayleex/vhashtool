package core

import (
	"bufio"
	"encoding/csv"
	"os"
)

type CSVWriter struct {
	w *csv.Writer
	f *os.File
}

func NewCSVWriter(path string) (*CSVWriter, error) {
	out, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	bw := bufio.NewWriter(out)
	cw := csv.NewWriter(bw)
	return &CSVWriter{
		w: cw,
		f: out,
	}, nil
}

func (x *CSVWriter) WriteRecord(record []string) {
	x.w.Write(record)
	x.w.Flush()
}

func (x *CSVWriter) CloseFile() {
	x.f.Close()
}

type BufferedCSVWriter struct {
	w     *csv.Writer
	f     *os.File
	queue chan []string
	quit  chan struct{}
}

func NewBufferedCSVWriter(bufSize int, path string) (*BufferedCSVWriter, error) {
	out, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	bw := bufio.NewWriter(out)
	cw := csv.NewWriter(bw)
	return &BufferedCSVWriter{
		w:     cw,
		f:     out,
		queue: make(chan []string, bufSize),
		quit:  make(chan struct{}),
	}, nil
}

func (x *BufferedCSVWriter) PushRecord(record []string) {
	x.queue <- record
}

func (x *BufferedCSVWriter) Run() {
	for record := range x.queue {
		x.w.Write(record)
		x.w.Flush()
	}
	x.quit <- struct{}{}
}

func (x *BufferedCSVWriter) Stop() {
	close(x.queue)
	<-x.quit
	close(x.quit)
	x.f.Close()
}
