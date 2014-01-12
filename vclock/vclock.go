package vclock

import (
	"fmt"
	"sort"
	"time"
)

// { "client id": { "counter": x, "timestamp": x } }
type Entry struct {
	counter   int
	timestamp int64
}
type VClock map[string]Entry

// Single VClock functions/methods

func now() int64 {
	return time.Now().UnixNano()
}

func Fresh() VClock {
	return VClock{}
}

func New(client string) VClock {
	return VClock{client: {1, now()}}
}

func (vc *VClock) Set(client string, counter int) {
	var entry = Entry{counter, now()}
	(*vc)[client] = entry
}

func (vc *VClock) Increment(client string) {
	entry := (*vc)[client]
	entry.counter += 1
	entry.timestamp = now()
	(*vc)[client] = entry
}

// Printing a VClock.

type clientPretty struct {
	c string
	e Entry
}
type byClient []*clientPretty

func (b byClient) Len() int           { return len(b) }
func (b byClient) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byClient) Less(i, j int) bool { return b[i].c < b[j].c }

func (vc VClock) String() string {
	vlist := byClient{}
	flist := [](struct {
		string
		int
	}){}
	for k, v := range vc {
		vlist = append(vlist, &clientPretty{k, v})
	}
	sort.Sort(vlist)
	for _, d := range vlist {
		x := new(struct {
			string
			int
		})
		x.string = d.c
		x.int = d.e.counter
		flist = append(flist, *x)
	}

	str := fmt.Sprintf("%v", flist)
	return str
}

// Comparing VClocks

func fieldGT(field string, a, b VClock) int {
	if a[field].counter > b[field].counter {
		return 1
	}
	return 0
}

// Compares two VClocks.
// If A is a descendant of B, returns 1.
// If B is a descendant of A, returns -1.
// If A == B, or A and B have split from a common ancestor, returns 0.
// A split history is a Riak 'sibling' case.
func Compare(a, b VClock) int {
	accA, accB := 0, 0
	for f := range a {
		accA += fieldGT(f, a, b)
	}
	for f := range b {
		accB += fieldGT(f, b, a)
	}

	// No sign function in the standard library?
	sign := func(i int) int {
		switch {
		case i < 0:
			return -1
		case i == 0:
			return 0
		case i > 0:
			return 1
		}
		return 0
	}

	return sign(accA) - sign(accB)
}

func Equal(a, b VClock) bool {
	for f := range a {
		switch {
		case b[f] == Entry{}:
			return false
		case b[f].counter != a[f].counter:
			return false
		}
	}
	for f := range b {
		switch {
		case a[f] == Entry{}:
			return false
		case a[f].counter != a[f].counter:
			return false
		}
	}
	return true
}

// Is A is a descendant of B?
func Descends(a, b VClock) bool {
	cmp := Compare(a, b)
	switch {
	case cmp == 1:
		return true
	case cmp == 0 && Equal(a, b):
		return true
	}
	// if cmp == 1 || (cmp == 0 && Equal(a, b)) { return true }
	return false
}

func MergeSelf(clocks []VClock, self string) VClock {
	clock := Merge(clocks)
	clock.Increment(self)
	return clock
}

func Merge(clocks []VClock) VClock {
	max := func(a, b int) int {
		if a > b {
			return a
		}
		return b
	}
	max64 := func(a, b int64) int64 {
		if a > b {
			return a
		}
		return b
	}
	acc := Fresh()
	for _, clock := range clocks {
		for client, entry := range clock {
			acc_client := acc[client]
			acc_client.counter = max(acc_client.counter, entry.counter)
			acc_client.timestamp = max64(acc_client.timestamp, entry.timestamp)
			acc[client] = acc_client
		}
	}

	return acc
}
