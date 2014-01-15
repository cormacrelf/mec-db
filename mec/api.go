package main

import (
	"fmt"
	"net/http"
	// "strconv"
	"github.com/codegangsta/martini"
	"github.com/cormacrelf/mec-db/peers"
	"github.com/cormacrelf/mec-db/store"
	"github.com/jmhodges/levigo"
)

// The MecDB embedded martini webserver

func GetRoot(db *levigo.DB, enc Encoder, params martini.Params) (int, string) {
	return 200, "stub"
}

func Get(pl *peers.PeerList, db *levigo.DB, enc Encoder, params martini.Params) (int, string) {
	key, _ := params["key"]
	ro := levigo.NewReadOptions()
	defer ro.Close()
	al, err := db.Get(ro, []byte(key))
	if err != nil || al == nil {
		return http.StatusNotFound, Must(enc.Encode(
			NewError(ErrCodeNotExist, fmt.Sprintf("key %s does not exist", params["key"]))))
	}
	// return http.StatusOK, Must(enc.Encode(al))
	return http.StatusOK, string(al)
}

func Post(db *levigo.DB, w *store.Store, enc Encoder, params martini.Params) (int, string) {
	key, _ := params["key"]
	value, _ := params["value"]
	client, _ := params["client"]
	vclock, _ := params["vclock"]
	// ro := levigo.NewReadOptions()
	// defer ro.Close()
	
	err := w.APIWrite(key, value, client, vclock)
	if err != nil {
		return http.StatusNotFound, Must(enc.Encode(
			NewError(ErrCodeNotExist, fmt.Sprintf("write failed to key '%s'", params["key"]))))
	}
	return http.StatusOK, Must(enc.Encode(""))
}
func Put(db *levigo.DB, enc Encoder, params martini.Params) (int, string) {
	return 200, "stub"
}
func Delete(db *levigo.DB, enc Encoder, params martini.Params) (int, string) {
	return 200, "stub"
}
