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
	R = 2
	W = 2
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
		case  msg := <-gets:
			// Respond to GET messages with FAIL or DATA
			key := parseGetMsg(false, msg...)
			value, content_type, vc, err := w.DBRead(key)
			r, err := encodeDataMsg("DATA", key, value, content_type, vc)
			reply := append([]string{msg[0]}, r...)
			if err != nil {
				w.pl.Reply(msg[0], "FAIL")
			}
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
	fmt.Printf("will write: %v %v %v %v\n", key, value, content_type, vc)
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

// APIRead returns value for key + a base64-encoded VClock
func (s Store) APIRead(key, client_id string) (string, string, string, error) {
	val, content_type, vc, err_read := s.DistributeRead(key)
	b64, err := encodeVClock(vc)
	if err_read != nil {
		return val, content_type, b64, err_read
	}
	if err != nil {
		b64, _ = encodeVClock(vclock.Fresh())
		return val, content_type, b64, err
	}

	return val, content_type, b64, nil
}

// Performs a Read-Repair on the key and returns a merged value
func (s Store) DistributeRead(key string) (string, string, vclock.VClock, error) {
	msg:= encodeGetMsg(key)
	responses, n := s.pl.RandomResponses(R, msg...)
	if n < R {
		if n >= 1 {
			for _, v := range responses {
				_, value, content_type, clock, err := parseDataMsg(false, v...)
				if err != nil {
					continue
				}
				return value, content_type, clock, errors.New("found data but repair failed")
				break
			}
		}
		return "", "", nil, errors.New(fmt.Sprintf("not enough successful reads: %d", n))
	}
	clockmap := make(map[string]vclock.VClock, n)
	// get a vclock for each response
	for k, msg := range responses {
		_, _, _, vc, _ := parseDataMsg(true, msg...)
		clockmap[k] = vc
	}

	latest := vclock.Latest(clockmap)
	clocks := make([]vclock.VClock, len(latest))
	i := 0
	for _, v := range latest {
		clocks[i] = v
		i++
	}

	if len(latest) > 1 && !vclock.AllEqual(clocks) {
		// TODO: we have siblings! handle it with multiple responses in future
		return "", "", nil, errors.New("siblings")
	} else {
		// now we have a list of equivalent clocks, pick one
		var (
			nodename string
			clock vclock.VClock
		)
		for k, v := range latest {
			nodename, clock = k, v
		}

		outdated := vclock.MapOutdated(clockmap)

		key, value, content_type, _, err := parseDataMsg(true, responses[nodename]...)
		if err != nil {
			return "", "", nil, errors.New("unparseable")
		}
		fmt.Printf("clockmap: %v\n\n", clockmap)
		fmt.Printf("outdated: %v\n\n", outdated)
		for i, node := range outdated {

			msg, err := encodeWriteMsg(key, value, content_type, clock)
			if err != nil {
				if i == len(outdated) {
					return "", "", nil, errors.New("unparseable")
				}
				continue
			}

			go s.pl.MessageExpectResponse(node, time.Second, msg...)
			// If they are unable to repair...
			// Who cares? That's not my fault.
		}

		return value, content_type, clock, nil
	}

	// That should repair everything.
	// And we never get here

	return "", "", nil, nil
}

// Write to the database
func (s Store) DBRead(key string) (string, string, vclock.VClock, error) {
	obj, err := s.db.Get(s.ro, []byte(key))
	if err != nil {
		fmt.Printf("write failed: %v", err)
		return "", "", nil, err
	}
	st, err := decodeStorable(obj)
	if err != nil {
		fmt.Printf("value decode failed: %v", err)
		return "", "", nil, err
	}

	return st.Value, st.Content_Type, st.VC, nil
}
