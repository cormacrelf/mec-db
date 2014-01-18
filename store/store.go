package store

import (
	"errors"
	"fmt"
	"github.com/cormacrelf/mec-db/peers"
	"github.com/cormacrelf/mec-db/vclock"
	"github.com/jmhodges/levigo"
	"time"
)

// handles
// - receiving node as coordinator on write
// - writing simple PUT from coordinator node
// - responding to PUT requests
// - fire PUT to W nodes
// - confirm success by examining replies
// - actual leveldb writes
// - VClocks at every stage

const (
	N = 3
	R = 3
	W = 1
)

type Store struct {
	ro *levigo.ReadOptions
	wo *levigo.WriteOptions
	db *levigo.DB
	pl *peers.PeerList
}

func Create(db *levigo.DB, pl *peers.PeerList) *Store {
	s := Store{
		ro: levigo.NewReadOptions(),
		wo: levigo.NewWriteOptions(),
		db: db,
		pl: pl,
	}

	go s.Listen()

	return &s
}

func (w Store) Listen() {
	writes := make(chan []string, 1000)
	w.pl.Subscribe(writes, "WRITE")
	gets := make(chan []string, 1000)
	w.pl.Subscribe(gets, "GET")
	for {
		select {
		case msg := <-writes:
			// fmt.Printf("store received: %v\n", msg)
			key, value, content_type, vc, err := parseWriteMsg(false, msg...)
			if err != nil {
				// handle error
				// or just silently drop?
			} else {
				err = w.DBWrite(key, value, content_type, vc)
				if err != nil {
					// reply with fail
					w.pl.Reply(msg[0], "FAIL")
				} else {
					w.pl.Reply(msg[0], "GOOD")
				}
			}
		case msg := <-gets:
			// Respond to GET messages with FAIL or DATA
			key := parseGetMsg(false, msg...)
			value, content_type, vc, err := w.DBRead(key)
			if err != nil {
				w.pl.Reply(msg[0], "FAIL")
				continue
			}
			r, err := encodeDataMsg("DATA", key, value, content_type, vc)
			if err != nil {
				w.pl.Reply(msg[0], "FAIL")
				continue
			}
			reply := append([]string{msg[0]}, r...)
			w.pl.Reply(reply...)
		}
	}
}

// APIWrite takes a client request and distributes it to itself and W-1 servers.
func (s Store) APIWrite(key, value, content_type, client_id, packed_vclock string) (string, error) {
	vc, err := parseVClock(packed_vclock)
	if err != nil {
		// handle the bad VClock input by making a new one
		vc = vclock.Fresh()
	}

	vc.Increment(client_id)

	err = s.DistributeWrite(key, value, content_type, vc)
	if err != nil {
		// nothing happened, give back the original clock
		return packed_vclock, err
	}

	b64, err := encodeVClock(vc)
	if err != nil {
		return packed_vclock, err
	}

	return b64, nil
}

func (s Store) DistributeWrite(key, value, content_type string, vc vclock.VClock) error {
	msg, err := encodeWriteMsg(key, value, content_type, vc)
	if err != nil {
		return err // fail here so we don't send unintelligible messages
	}
	n := s.pl.VerifyRandom(W, msg...)
	if n < W {
		return errors.New("not enough successful writes")
	}
	return nil
}

// Write to the database
func (s Store) DBWrite(key, value, content_type string, vc vclock.VClock) error {
	fmt.Printf("will write: %v \"%v\" %v %v\n", key, value, content_type, vc)
	obj, err := encodeStorable(Storable{value, content_type, vc})
	if err != nil {
		fmt.Printf("write failed: %v", err)
		return err
	}

	err = s.db.Put(s.wo, []byte(key), obj)
	if err != nil {
		fmt.Printf("write failed: %v", err)
		return err
	}
	return nil
}

type ReadValue struct {
	Value        string
	Content_Type string
	Timestamp    int64
}

type MaybeMulti struct {
	Multi    bool
	Single   ReadValue   // if Multi then == nil
	Multiple []ReadValue // if not Multi then == nil
}

// APIRead returns value for key + a base64-encoded VClock
func (s Store) APIRead(key, client_id string) (MaybeMulti, string, error) {
	maybe, vc, err_read := s.DistributeRead(key)
	b64, err := encodeVClock(vc)
	if err_read != nil {
		return maybe, b64, err_read
	}
	if err != nil {
		b64, _ = encodeVClock(vclock.Fresh())
		return maybe, b64, err
	}

	return maybe, b64, nil
}

// Performs a Read-Repair on the key and returns a merged value
func (s Store) DistributeRead(key string) (MaybeMulti, vclock.VClock, error) {
	msg := encodeGetMsg(key)
	responses, n := s.pl.RandomResponses(R, msg...)
	if n < R {
		if n >= 1 {
			for _, v := range responses {
				_, value, content_type, clock, err := parseDataMsg(false, v...)
				if err != nil {
					continue
				}
				return MaybeMulti{false, ReadValue{value, content_type, clock.MaxTimestamp()}, nil}, clock, errors.New("found data but repair failed")
				break
			}
		}
		return MaybeMulti{}, nil, errors.New(fmt.Sprintf("not enough successful reads: %d", n))
	}
	data := make(map[string]ReadValue, n)         // map responses to returnable values
	clockmap := make(map[string]vclock.VClock, n) // map responses to vclocks

	// take vclock for each response into clockmap
	for k, msg := range responses {
		_, v, c, vc, _ := parseDataMsg(true, msg...)
		clockmap[k] = vc
		data[k] = ReadValue{v, c, vc.MaxTimestamp()}
	}

	// get map of responses to the latest (potentially sibling) clocks
	latest := vclock.Latest(clockmap)
	// clocks makes it convenient to do ops on clocks
	clocks := make([]vclock.VClock, len(latest))
	i := 0
	for _, v := range latest {
		clocks[i] = v
		i++
	}

	if len(latest) > 1 && !vclock.AllEqual(clocks) {
		// we have siblings!
		// get a merged clock to return to client
		merged := vclock.Merge(clocks)
		// get a list of returnable values in admittedly random order
		returnables := make([]ReadValue, len(latest))
		i := 0
		for k, _ := range latest {
			returnables[i] = data[k]
			i++
		}

		return MaybeMulti{Multi: true, Single: ReadValue{}, Multiple: returnables}, merged, nil
	} else {
		// now we have a list of equivalent clocks, pick one
		var (
			nodename string
			clock    vclock.VClock
		)
		for k, v := range latest {
			nodename, clock = k, v
		}

		outdated := vclock.MapOutdated(clockmap)

		key, value, content_type, _, err := parseDataMsg(true, responses[nodename]...)
		if err != nil {
			return MaybeMulti{}, nil, errors.New("unparseable")
		}
		for i, node := range outdated {

			msg, err := encodeWriteMsg(key, value, content_type, clock)
			if err != nil {
				if i == len(outdated) {
					return MaybeMulti{}, nil, errors.New("unparseable")
				}
				continue
			}

			go s.pl.MessageExpectResponse(node, time.Second, msg...)
			// If they are unable to repair...
			// Who cares? That's not my fault.
		}

		rv := ReadValue{value, content_type, clock.MaxTimestamp()}
		maybe := MaybeMulti{false, rv, nil}

		return maybe, clock, nil
	}

	// That should repair everything.
	// And we never get here

	return MaybeMulti{}, nil, nil
}

// Write to the database
func (s Store) DBRead(key string) (string, string, vclock.VClock, error) {
	obj, err := s.db.Get(s.ro, []byte(key))
	if err != nil {
		fmt.Printf("read failed: %v", err)
		return "", "", nil, err
	}
	st, err := decodeStorable(obj)
	if err != nil {
		fmt.Printf("value decode failed: %v", err)
		return "", "", nil, err
	}

	return st.Value, st.Content_Type, st.VC, nil
}
