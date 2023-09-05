package core

import (
	"bytes"
	"encoding/hex"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"time"
)

func NewCRCWriter(poly uint32, w io.Writer) *CRCWriter {
	return &CRCWriter{
		h: crc32.New(crc32.MakeTable(poly)),
		w: w,
	}
}

type CRCWriter struct {
	h hash.Hash32
	w io.Writer
}

func (c *CRCWriter) Write(p []byte) (n int, err error) {
	n, err = c.w.Write(p)
	c.h.Write(p)
	return
}

func (c *CRCWriter) Sum() []byte {
	defer c.h.Reset()
	return c.h.Sum(nil)[:]
}

type ChecksumWriter interface {
	io.Writer
	Sum() []byte
}

type ChecksumGenerator struct {
	writer   ChecksumWriter
	buffer   *bytes.Buffer
	ReadTime time.Duration
	CalcTime time.Duration
}

func NewChecksumGenerator() *ChecksumGenerator {
	p := make([]byte, 1024*1024)
	buffer := bytes.NewBuffer(p)
	writer := NewCRCWriter(crc32.IEEE, buffer)
	return &ChecksumGenerator{
		writer:   writer,
		buffer:   buffer,
		ReadTime: 0,
		CalcTime: 0,
	}

}

func (x *ChecksumGenerator) Gen(path string) (string, error) {
	defer x.buffer.Reset()
	rs := time.Now()
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	_, err = io.Copy(x.writer, file)
	re := time.Now()
	d := re.Sub(rs)
	x.ReadTime += (d / 1000)

	if err != nil {
		return "", err
	}
	sum := hex.EncodeToString(x.writer.Sum())
	d = time.Since(re)
	x.CalcTime += (d / 1000)
	return sum, nil
}
