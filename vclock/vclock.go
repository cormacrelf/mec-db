package vclock

import (
	"time"
	"sort"
	"fmt"
)

// { "client id": { "counter": x, "timestamp": x } }
type Entry struct {counter int; timestamp int64}
type VClock map[string]Entry

// Single VClock functions/methods

func now() int64 {
	return time.Now().UnixNano()
}

func Fresh() VClock {
	return VClock {}
}

func New(client string) VClock {
	return VClock {client: {1, now()}}
}

func (vc *VClock) Set(client string, counter int) {
	var entry = Entry { counter, now() }
	(*vc)[client] = entry
}

func (vc *VClock) Increment(client string) {
	entry := (*vc)[client]
	entry.counter += 1
	entry.timestamp = now()
	(*vc)[client] = entry
}

// Printing a VClock.

type clientPretty struct{c string; e Entry}
type byClient []*clientPretty
func (b byClient) Len() int { return len(b) }
func (b byClient) Swap(i,j int) { b[i], b[j] = b[j], b[i] }
func (b byClient) Less(i,j int) bool { return b[i].c < b[j].c }

func (vc VClock) String() string {
	vlist := byClient {}
	flist := [](struct{string; int}) {}
	for k, v := range vc {
		vlist = append(vlist, &clientPretty{k, v})
	}
	sort.Sort(vlist)
	for _, d := range vlist {
		x := new(struct{string; int})
		x.string = d.c
		x.int = d.e.counter
		flist = append(flist, *x)
	}

	str := fmt.Sprintf("%v", flist)
	return str
}

