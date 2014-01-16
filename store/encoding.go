package store

import (
	"encoding/base64"
	"errors"
	"github.com/cormacrelf/mec-db/vclock"
	"github.com/ugorji/go/codec"
	"reflect"
)

// Takes message parts and returns key, value, VClock
func parseMsg(naked bool, msg ...string) (string, string, vclock.VClock, error) {
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
func encodeMsg(key, value string, vc vclock.VClock) ([]string, error) {
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

type Storable struct {
	Value string
	VC  vclock.VClock
}

func encodeStorable(wr Storable) ([]byte, error) {
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

func decodeStorable(data []byte) (Storable, error) {
	var mh codec.MsgpackHandle
	var wr Storable
	mh.MapType = reflect.TypeOf(wr)

	dec := codec.NewDecoderBytes(data, &mh)
	err := dec.Decode(&wr)
	if err != nil {
		return Storable{}, errors.New("writable not decoded")
	}

	return wr, nil
}

