package vclock

import (
	"fmt"
	"sort"
	"time"
)

// { "Counter": x, "Timestamp": x }
type Entry struct {
	Counter   int
	Timestamp int64
}

// { "client id": { "Counter": x, "Timestamp": x } }
type VClock map[string]Entry

// Single VClock functions/methods

func now() int64 {
	return time.Now().UnixNano()
}

// Fresh = no entries
func Fresh() VClock {
	return VClock{}
}

// Give us a new VClock with the given client = 1
func New(client string) VClock {
	return VClock{client: {1, now()}}
}

// Set a counter for the given client. Not useful for production.
func (vc *VClock) Set(client string, counter int) {
	var entry = Entry{counter, now()}
	(*vc)[client] = entry
}

// Increment the given client's counter and update its timestamp
func (vc *VClock) Increment(client string) {
	entry := (*vc)[client]
	entry.Counter += 1
	entry.Timestamp = now()
	(*vc)[client] = entry
}

// IsValid validates a clock by checking:
// * ID is not a zero-length string
// * Each counter > 0
// * Each timestamp has been updated (> 0)
func (vc VClock) IsValid() bool {
	for k, v := range vc {
		if k == "" {
			return false
		} else if v.Counter <= 0 {
			return false
		} else if v.Timestamp <= 0 {
			return false
		}
	}
	return true
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

// Pretty-print formats a VClock so we can read logs and not deal with
// excessively long timestamps
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
		x.int = d.e.Counter
		flist = append(flist, *x)
	}

	str := fmt.Sprintf("%v", flist)
	return str
}

// Comparing VClocks

func fieldGT(field string, a, b VClock) int {
	if a[field].Counter > b[field].Counter {
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

// Equal compares only IDs and their counters, not the times at which
// they were updated. This is because a different server will have a
// different clock and receive a write at a different time anyway.
func Equal(a, b VClock) bool {
	for f := range a {
		switch {
		case b[f] == Entry{}:
			return false
		case b[f].Counter != a[f].Counter:
			return false
		}
	}
	for f := range b {
		switch {
		case a[f] == Entry{}:
			return false
		case a[f].Counter != a[f].Counter:
			return false
		}
	}
	return true
}

// Is A is a descendant of B?
// Note: if A == B, then A also descends B.
func Descends(a, b VClock) bool {
	cmp := Compare(a, b)
	switch {
	case cmp == 1:
		return true
	case cmp == 0 && Equal(a, b):
		return true
	}
	// otherwise, B descends A
	return false
}

// Determines if A is behind any other clock
func Outdated(A VClock, clocks []VClock) bool {
	acc := false
	for _, clock := range clocks {
		if Compare(A, clock) == -1 {
			acc = true
		}
	}
	return acc
}

// Takes a map of strings (nodes) to VClocks and returns strings whose clocks
// are out of date.
func MapOutdated(nodes map[string]VClock) []string {
	clocks := make([]VClock, len(nodes))
	i := 0
	for _, v := range nodes {
		clocks[i] = v
		i++
	}
	acc := make([]string, 0)
	for k, v := range nodes {
		if Outdated(v, clocks) {
			acc = append(acc, k)
		}
	}

	return acc
}

// Latest finds the most logically recent set of clocks in a map and returns
// the one with the latest timestamp.
func Latest(nodes map[string]VClock) (map[string]VClock) {
	latest := make(map[string]VClock, len(nodes))
	clocks := make([]VClock, len(nodes))
	i := 0
	for _, v := range nodes {
		clocks[i] = v
		i++
	}
	for k, v := range nodes {
		if !Outdated(v, clocks) {
			latest[k] = v
		}
	}

	return latest
}

func (vc VClock) MaxTimestamp() int64 {
	max64 := func(a, b int64) int64 {
		if a > b {
			return a
		}
		return b
	}

	acc := int64(0)
	for _, v := range vc {
		acc = max64(acc, v.Timestamp)
	}
	return acc
}

// AllEqual returns true if every given clock is equal
func AllEqual(clocks []VClock) bool {
	for i, v := range clocks {
		for j, other := range clocks {
			if i < j {
				if !Equal(v, other) {
					return false
				}
			}
		}
	}
	return true
}

// Merge a list of clocks and increment own counter
func MergeSelf(clocks []VClock, self string) VClock {
	clock := Merge(clocks)
	clock.Increment(self)
	return clock
}

// Merge a list of VClocks by finding a clock that descends every one
// of them. That means each ID's counter in the new clock will be the
// max() of all counters for that ID in each input clock.
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
			acc_client.Counter = max(acc_client.Counter, entry.Counter)
			acc_client.Timestamp = max64(acc_client.Timestamp, entry.Timestamp)
			acc[client] = acc_client
		}
	}

	return acc
}

