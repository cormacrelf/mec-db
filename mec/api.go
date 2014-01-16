package main

import (
	"fmt"
	"net/http"
	// "strconv"
	"github.com/codegangsta/martini"
	"github.com/cormacrelf/mec-db/store"
	"github.com/jmhodges/levigo"
)

// The MecDB embedded martini webserver

func GetRoot(db *levigo.DB, enc Encoder, params martini.Params) (int, string) {
	return 200, "stub"
}

func Get(s *store.Store, enc Encoder, params martini.Params, w http.ResponseWriter, r *http.Request) (int, string) {
	key, _ := params["key"]
	client, _ := params["client"]
	val, _, err := s.APIRead(key, client)
	if err != nil || val == "" {
		return http.StatusNotFound, Must(enc.Encode(
			NewError(ErrCodeNotExist, fmt.Sprintf("key %s does not exist", params["key"]))))
	}
	// return http.StatusOK, Must(enc.Encode(al))
	return http.StatusOK, string(val)
}

func Post(s *store.Store, enc Encoder, params martini.Params, w http.ResponseWriter, r *http.Request) (int, string) {
	key, _ := params["key"]
	value, _ := params["value"]
	client, _ := params["client"]
	vclock, _ := params["vclock"]

	err := s.APIWrite(key, value, client, vclock)
	if err != nil {
		return http.StatusNotFound, Must(enc.Encode(
			NewError(ErrCodeNotExist, fmt.Sprintf("write failed to key '%s'", params["key"]))))
	}
	return http.StatusOK, Must(enc.Encode(""))
}
func Put(db *levigo.DB, enc Encoder, params martini.Params, w http.ResponseWriter, r *http.Request) (int, string) {
	return 200, "stub"
}
func Delete(db *levigo.DB, enc Encoder, params martini.Params, w http.ResponseWriter, r *http.Request) (int, string) {
	return 200, "stub"
}

// MapEncoder intercepts the request's URL, detects the requested format,
// and injects the correct encoder dependency for this request. It rewrites
// the URL to remove the format extension, so that routes can be defined
// without it.
func MapEncoder(c martini.Context, w http.ResponseWriter, r *http.Request) {
	c.MapTo(jsonEncoder{}, (*Encoder)(nil))
	w.Header().Set("Content-Type", "application/json")
}

