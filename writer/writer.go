package writer

import (
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/cormacrelf/mec-db/peers"
	"github.com/cormacrelf/mec-db/vclock"
	"github.com/jmhodges/levigo"
	"github.com/ugorji/go/codec"
	"reflect"
)

// handles
// - receiving node as coordinator on write
// - writing simple PUT from coordinator node
// - responding to PUT requests
// - fire PUT to W nodes
// - confirm success by examining replies
// - actual leveldb writes
// - VClocks at every stage

type Writer struct {
	ro *levigo.ReadOptions
	wo *levigo.WriteOptions
	db *levigo.DB
	pl *peers.PeerList
}

func Create(db *levigo.DB, pl *peers.PeerList) *Writer {
	w := Writer{
		ro: levigo.NewReadOptions(),
		wo: levigo.NewWriteOptions(),
		db: db,
		pl: pl,
	}

	// msg := []string{"random nonsense", "WRITE", "key", "value", "VClock"}
	// msg, err := encode("key", "value", vclock.New("cormac"))
	// fmt.Println(msg)
	// key, value, vc, err := parse(true, msg...)
	// fmt.Printf("%s: %s - %v - %v", key, value, vc, err)

	return &w
}

func (w Writer) Listen() {
	incoming := make(chan []string, 1000)
	w.pl.Subscribe(incoming, "WRITE")
	for {
		msg := <-incoming
		key, value, vc, err := parse(false, msg...)
		if err != nil {
			// handle error
			// or just silently drop?
		} else {
			w.DBWrite(key, value, vc)
		}
	}
}

// Takes message parts and returns key, value, VClock
func parse(naked bool, msg ...string) (string, string, vclock.VClock, error) {
	var mh codec.MsgpackHandle
	var vc vclock.VClock
	mh.MapType = reflect.TypeOf(vc)

	var ia int
	if naked {
		ia = 0
	} else {
		ia = 1 // get past ROUTER's routing data
	}
	// key,	   value,  VClock
	// string, string, []byte  ... msg[ia] == "WRITE"
	key, value, b := msg[ia+1], msg[ia+2], []byte(msg[ia+3])

	dec := codec.NewDecoderBytes(b, &mh)
	err := dec.Decode(&vc)
	if err != nil {
		return key, value, nil, errors.New("VClock not parsed")
	}

	return key, value, vc, nil
}

func parseVClock(encoded string) (vclock.VClock, error) {
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return vclock.VClock{}, errors.New("couldn't base64decode input")
	}

	var mh codec.MsgpackHandle
	var vc vclock.VClock
	mh.MapType = reflect.TypeOf(vc)

	dec := codec.NewDecoderBytes(data, &mh)
	err = dec.Decode(&vc)
	if err != nil {
		return vclock.VClock{}, errors.New("VClock not parsed")
	}

	return vc, nil
}

func encodeVClock(vc vclock.VClock) (string, error) {
	var mh codec.MsgpackHandle
	var data []byte
	mh.MapType = reflect.TypeOf(vc)

	enc := codec.NewEncoderBytes(&data, &mh)
	err := enc.Encode(vc)
	if err != nil {
		return "", errors.New("could not encode VClock")
	}

	str := base64.StdEncoding.EncodeToString(data)

	return str, nil
}

// Encode message parts into sendable zeromq message
func encode(key, value string, vc vclock.VClock) ([]string, error) {
	var mh codec.MsgpackHandle
	var b []byte


	mh.MapType = reflect.TypeOf(vc)

	enc := codec.NewEncoderBytes(&b, &mh)
	err := enc.Encode(vc)
	if err != nil {
		return nil, errors.New("VClock not encoded")
	}

	msg := make([]string, 4, 4)
	msg[0], msg[1], msg[2], msg[3] = "WRITE", key, value, string(b)

	return msg, nil
}

type Writable struct {
	value string
	vc  vclock.VClock
}

func encodeWritable(wr Writable) ([]byte, error) {
	var mh codec.MsgpackHandle
	var b []byte

	mh.MapType = reflect.TypeOf(wr)

	enc := codec.NewEncoderBytes(&b, &mh)
	err := enc.Encode(wr)
	if err != nil {
		return nil, errors.New("failed to encode Writable")
	}
	return b, nil
}

func decodeWritable(data []byte) (Writable, error) {
	var mh codec.MsgpackHandle
	var wr Writable
	mh.MapType = reflect.TypeOf(wr)

	dec := codec.NewDecoderBytes(data, &mh)
	err := dec.Decode(&wr)
	if err != nil {
		return Writable{}, errors.New("writable not decoded")
	}

	return wr, nil
}

func (w Writer) APIWrite(key, value, client_id, packed_vclock string) error {
	vc, err := parseVClock(packed_vclock)
	if err != nil {
		// handle the bad VClock input by making a new one
		vc = vclock.Fresh()
	}
	vc.Increment(client_id)

	err = w.DBWrite(key, value, vc)
	if err != nil {
		return err
	}

	return nil
}

// Write to the database
func (w Writer) DBWrite(key, value string, vc vclock.VClock) error {
	obj, err := encodeWritable(Writable{value, vc})
	if err != nil {
		fmt.Printf("write failed: %v", err)
		return err
	}

	err = w.db.Put(w.wo, []byte(key), obj)
	if err != nil {
		fmt.Printf("write failed: %v", err)
		return err
	}
	return nil
}
