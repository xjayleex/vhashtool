package core

import (
	"math/rand"
	"sort"
	"time"
)

// args => n int, fi
type Sampler struct {
}

func (x *Sampler) GetSamples(n int, total int) []int {
	if n > total {
		return []int{}
	}
	rand.Seed(time.Now().UnixNano())
	perm := rand.Perm(total)[0:n]
	sort.Ints(perm)
	return perm
}

/*
func (x *Sampler) GetSamplesWithStartPoint(n, total, sp int) []int {
	if n > total {
		return []int{}
	}
	rand.Seed(time.Now().UnixNano())
	perm := rand.Perm(total)
}*/
