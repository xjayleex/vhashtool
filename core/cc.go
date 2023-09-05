package core

import (
	"errors"
	"sync"
)

type fileTicketPub struct {
	mu     sync.Mutex
	list   []FileInfo
	index  int
	length int
}

func NewFileTicketPub(list []FileInfo) *fileTicketPub {
	return &fileTicketPub{
		mu:     sync.Mutex{},
		list:   list,
		index:  0,
		length: len(list),
	}
}

func (x *fileTicketPub) GetTicket() (f *FileInfo, err error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.index >= x.length {
		return nil, errors.New("")
	}
	f = &x.list[x.index]
	x.index++
	return f, nil
}
