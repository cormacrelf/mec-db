package store

import (
	"encoding/base64"
	"fmt"
	"errors"
	"github.com/cormacrelf/mec-db/vclock"
	"github.com/ugorji/go/codec"
	"reflect"
)

// Takes <any command> message parts and returns key, value, content_type, VClock
func parseDataMsg(naked bool, msg ...string) (string, string, string, vclock.VClock, error) {
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
	key, value, content_type, b := msg[ia+1], msg[ia+2], msg[ia+3], []byte(msg[ia+4])

	dec := codec.NewDecoderBytes(b, &mh)
	err := dec.Decode(&vc)
	if err != nil {
		return key, value, content_type, nil, errors.New("VClock not parsed")
	}

	return key, value, content_type, vc, nil
}

// Encode <any cmd> message parts into sendable zeromq message
func encodeDataMsg(cmd, key, value, content_type string, vc vclock.VClock) ([]string, error) {
	var mh codec.MsgpackHandle
	var b []byte

	mh.MapType = reflect.TypeOf(vc)

	enc := codec.NewEncoderBytes(&b, &mh)
	err := enc.Encode(vc)
	if err != nil {
		return nil, errors.New("VClock not encoded")
	}

	msg := make([]string, 5)
	msg[0], msg[1], msg[2], msg[3], msg[4] = cmd, key, value, content_type, string(b)

	return msg, nil
}

// Takes WRITE message parts and returns key, value, content_type, VClock
func parseWriteMsg(naked bool, msg ...string) (string, string, string, vclock.VClock, error) {
	return parseDataMsg(naked, msg...)
}

// Encode WRITE message parts into sendable zeromq message
func encodeWriteMsg(key, value, content_type string, vc vclock.VClock) ([]string, error) {
	return encodeDataMsg("WRITE", key, value, content_type, vc)
}

// Takes message parts and returns key
func parseGetMsg(naked bool, msg ...string) (string) {
	var ia int
	if naked {
		ia = 0
	} else {
		ia = 1 // get past ROUTER's routing data
	}

	// "random" "GET" "key:string"
	return msg[ia+1]
}

// Encode GET message parts into sendable zeromq message
func encodeGetMsg(key string) ([]string) {
	msg := make([]string, 2)
	msg[0], msg[1] = "GET", key

	return msg
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

type Storable struct {
	Value        string
	Content_Type string
	VC           vclock.VClock
}

func encodeStorable(wr Storable) ([]byte, error) {
	var mh codec.MsgpackHandle
	var b []byte

	mh.MapType = reflect.TypeOf(wr)

	enc := codec.NewEncoderBytes(&b, &mh)
	err := enc.Encode(wr)
	if err != nil {
		return nil, errors.New("failed to encode Storable")
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
		return Storable{}, errors.New(fmt.Sprintf("storable not decoded: len:%d: %v\n", len(data), data))
	}

	return wr, nil
}
