package main

import (
	"fmt"
	"net/http"
	// "strconv"
	"github.com/codegangsta/martini"
	"github.com/cormacrelf/mec-db/store"
	"github.com/jmhodges/levigo"
	"io/ioutil"
)

// The MecDB embedded martini webserver

func GetRoot(db *levigo.DB, enc Encoder, params martini.Params) (int, string) {
	return 200, "stub"
}

func Get(s *store.Store, enc Encoder, params martini.Params, res http.ResponseWriter, req *http.Request) (int, string) {
	key, _ := params["key"]
	client := req.Header.Get("X-Client-ID")
	val, content_type, b64, err := s.APIRead(key, client)
	if err != nil || val == "" {
		// return http.StatusNotFound, Must(enc.Encode(
		// 	NewError(ErrCodeNotExist, fmt.Sprintf("key %s does not exist", params["key"]))))
		return 500, err.Error()
	}

	res.Header().Set("X-Mec-Vclock", b64)
	res.Header().Set("Content-Type", content_type)
	res.WriteHeader(200)

	return http.StatusOK, string(val)
}

func Post(s *store.Store, enc Encoder, params martini.Params, res http.ResponseWriter, req *http.Request) (int, string) {
	key, _ := params["key"]
	value, _ := ioutil.ReadAll(req.Body)
	content_type := req.Header.Get("Content-Type")
	client := req.Header.Get("X-Client-ID")
	fmt.Println(client)
	vclock := req.Header.Get("X-Mec-Vclock")

	b64, err := s.APIWrite(key, string(value), content_type, client, vclock)
	if err != nil {
		return http.StatusNotFound, Must(enc.Encode(
			NewError(ErrCodeNotExist, fmt.Sprintf("write failed to key '%s'", params["key"]))))
	}

	res.Header().Set("X-Mec-Vclock", b64)
	res.WriteHeader(200)

	return http.StatusOK, ""
}
func Put(db *levigo.DB, enc Encoder, params martini.Params, res http.ResponseWriter, req *http.Request) (int, string) {
	return 200, "stub"
}
func Delete(db *levigo.DB, enc Encoder, params martini.Params, res http.ResponseWriter, req *http.Request) (int, string) {
	return 200, "stub"
}

// MapEncoder intercepts the request's URL, detects the requested format,
// and injects the correct encoder dependency for this request. It rewrites
// the URL to remove the format extension, so that routes can be defined
// without it.
func MapEncoder(c martini.Context, res http.ResponseWriter, req *http.Request) {
	c.MapTo(jsonEncoder{}, (*Encoder)(nil))
	res.Header().Set("Content-Type", "application/json")
}

