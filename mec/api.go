package main

import (
	"bytes"
	"fmt"
	"github.com/codegangsta/martini"
	"github.com/cormacrelf/mec-db/store"
	"github.com/jmhodges/levigo"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"time"
)

// The MecDB embedded martini webserver

func GetRoot(db *levigo.DB, enc Encoder, params martini.Params) (int, string) {
	return 200, "stub"
}

func Get(s *store.Store, enc Encoder, params martini.Params, res http.ResponseWriter, req *http.Request) {
	key, _ := params["key"]
	client := req.Header.Get("X-Mec-Client-ID")
	maybe, b64, err := s.APIRead(key, client)
	res.Header().Set("X-Mec-Vclock", b64)

	if !maybe.Multi && err == nil {
		rv := maybe.Single
		res.Header().Set("Content-Type", rv.Content_Type)
		t := time.Unix(0, rv.Timestamp)
		res.Header().Set("Last-Modified", t.Format(http.TimeFormat))
		res.Header().Set("X-Mec-Timestamp", fmt.Sprintf("%d",rv.Timestamp))
		res.WriteHeader(http.StatusOK)
		res.Write([]byte(rv.Value))
		return
	}

	if maybe.Multi && err == nil {
		res.Header().Set("Content-Type", "mime/multipart")
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)
		for _, rv := range maybe.Multiple {
			header := make(textproto.MIMEHeader)
			header.Set("Content-Type", rv.Content_Type)
			t := time.Unix(0, rv.Timestamp)
			header.Set("Last-Modified", t.Format(http.TimeFormat))
			header.Set("X-Mec-Timestamp", fmt.Sprintf("%d",rv.Timestamp))
			iow, errc := writer.CreatePart(header)
			if errc != nil {
				continue
			}
			data := []byte(rv.Value)
			iow.Write(data)
		}

		res.WriteHeader(http.StatusMultipleChoices) // 300
		res.Write(body.Bytes())
		return
	}

	if err != nil {
		// return http.StatusNotFound, Must(enc.Encode(
		// 	NewError(ErrCodeNotExist, fmt.Sprintf("key %s does not exist", params["key"]))))
		res.WriteHeader(500)
		res.Write([]byte(err.Error()))
		return
	}

	res.WriteHeader(http.StatusOK)
	res.Write(nil)
	return
}

func Post(s *store.Store, enc Encoder, params martini.Params, res http.ResponseWriter, req *http.Request) (int, string) {
	return Put(s, enc, params, res, req)
}

func Put(s *store.Store, enc Encoder, params martini.Params, res http.ResponseWriter, req *http.Request) (int, string) {
	key, _ := params["key"]
	value, _ := ioutil.ReadAll(req.Body)
	content_type := req.Header.Get("Content-Type")
	client := req.Header.Get("X-Mec-Client-ID")
	vclock := req.Header.Get("X-Mec-Vclock")

	b64, err := s.APIWrite(key, string(value), content_type, client, vclock)
	if err != nil {
		return http.StatusNotFound, Must(enc.Encode(
			NewError(ErrCodeNotExist, fmt.Sprintf("write failed to key '%s'", params["key"]))))
	}

	res.Header().Set("X-Mec-Vclock", b64)

	return http.StatusOK, ""
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
	// res.Header().Set("Content-Type", "application/json")
}

