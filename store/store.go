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

	// msg := []string{"random nonsense", "WRITE", "key", "value", "VClock"}
	// msg, err := encode("key", "value", vclock.New("cormac"))
	// fmt.Println(msg)
	// key, value, vc, err := parseMsg(true, msg...)
	// fmt.Printf("%s: %s - %v - %v", key, value, vc, err)

	return &s
}

func (w Store) Listen() {
	incoming := make(chan []string, 1000)
	w.pl.Subscribe(incoming, "WRITE")
	for {
		msg := <-incoming
		key, value, vc, err := parseMsg(false, msg...)
		if err != nil {
			// handle error
			// or just silently drop?
		} else {
			w.DBWrite(key, value, vc)
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

	err = s.DBWrite(key, value, vc)
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
	n := s.pl.SendRandom(1, msg...)
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
