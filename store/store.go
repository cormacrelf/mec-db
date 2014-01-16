package store

import (
	"errors"
	"fmt"
	"github.com/cormacrelf/mec-db/peers"
	"github.com/cormacrelf/mec-db/vclock"
	"github.com/jmhodges/levigo"
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
			key, value, vc, err := parseMsg(false, msg...)
			if err != nil {
				// handle error
				// or just silently drop?
			} else {
				err = w.DBWrite(key, value, vc)
				if err != nil {
					// reply with fail
					w.pl.Reply(msg[0], "FAIL")
				} else {
					w.pl.Reply(msg[0], "GOOD")
				}
			}
		case  msg := <-gets:
			fmt.Printf("store received: %v\n", msg)
			key, value, vc, err := parseMsg(false, msg...)
			if err != nil {
				// handle error
				// or just silently drop?
			} else {
				fmt.Printf("will write: %v %v %v\n", key, value, vc)
				w.DBWrite(key, value, vc)
			}
		}
	}
}

// APIWrite takes a client request and distributes it to itself and W-1 servers.
func (s Store) APIWrite(key, value, client_id, packed_vclock string) error {
	vc, err := parseVClock(packed_vclock)
	if err != nil {
		// handle the bad VClock input by making a new one
		vc = vclock.Fresh()
	}
	vc.Increment(client_id)

	err = s.DistributeWrite(key, value, vc)
	if err != nil {
		return err
	}

	return nil
}

func (s Store) DistributeWrite(key, value string, vc vclock.VClock) error {
	msg, err := encodeMsg(key, value, vc)
	if err != nil {
		return err // fail here so we don't send unintelligible messages
	}
	n := s.pl.VerifyRandom(1, msg...)
	if n < 1 {
		return errors.New("not enough successful writes")
	}
	err = s.DBWrite(key, value, vc)
	if err != nil {
		return err
	}
	return nil
}

// Write to the database
func (s Store) DBWrite(key, value string, vc vclock.VClock) error {
	fmt.Printf("will write: %v %v %v\n", key, value, vc)
	obj, err := encodeStorable(Storable{value, vc})
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

// APIWrite returns value for key + a base64-encoded VClock
func (s Store) APIRead(key, client_id string) (string, string, error) {
	val, vc, err := s.DBRead(key)
	if err != nil {
		return "", "", err
	}
	b64, err := encodeVClock(vc)
	if err != nil {
		return val, "", err
	}

	return val, b64, err
}

func (s Store) DistributeRead(key string, vc vclock.VClock) error {
	msg, err := encodeMsg(key, key, vc)
	if err != nil {
		return err // fail here so we don't send unintelligible messages
	}
	n := s.pl.VerifyRandom(1, msg...)
	if n < 1 {
		return errors.New("not enough successful writes")
	}
	_, _, err = s.DBRead(key)
	if err != nil {
		return err
	}
	return nil
}

// Write to the database
func (s Store) DBRead(key string) (string, vclock.VClock, error) {
	fmt.Printf("will read: %v\n", key)

	obj, err := s.db.Get(s.ro, []byte(key))
	if err != nil {
		fmt.Printf("write failed: %v", err)
		return "", nil, err
	}
	st, err := decodeStorable(obj)
	if err != nil {
		fmt.Printf("value decode failed: %v", err)
		return "", nil, err
	}

	return st.Value, st.VC, nil
}
