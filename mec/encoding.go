/*
Copyright (c) 2013, Martin Angers
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

* Neither the name of the author nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package main

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
)

// An Encoder implements an encoding format of values to be sent as response to
// requests on the API endpoints.
type Encoder interface {
	Encode(v ...interface{}) (string, error)
}

// Because `panic`s are caught by martini's Recovery handler, it can be used
// to return server-side errors (500). Some helpful text message should probably
// be sent, although not the technical error (which is printed in the log).
func Must(data string, err error) string {
	if err != nil {
		panic(err)
	}
	return data
}

type jsonEncoder struct{}

// jsonEncoder is an Encoder that produces JSON-formatted responses.
func (_ jsonEncoder) Encode(v ...interface{}) (string, error) {
	var data interface{} = v
	if v == nil {
		// So that empty results produces `[]` and not `null`
		data = []interface{}{}
	} else if len(v) == 1 {
		data = v[0]
	}
	b, err := json.Marshal(data)
	return string(b), err
}

type xmlEncoder struct{}

// xmlEncoder is an Encoder that produces XML-formatted responses.
func (_ xmlEncoder) Encode(v ...interface{}) (string, error) {
	var buf bytes.Buffer
	if _, err := buf.Write([]byte(xml.Header)); err != nil {
		return "", err
	}
	if _, err := buf.Write([]byte("<albums>")); err != nil {
		return "", err
	}
	b, err := xml.Marshal(v)
	if err != nil {
		return "", err
	}
	if _, err := buf.Write(b); err != nil {
		return "", err
	}
	if _, err := buf.Write([]byte("</albums>")); err != nil {
		return "", err
	}
	return buf.String(), nil
}

type textEncoder struct{}

// textEncoder is an Encoder that produces plain text-formatted responses.
func (_ textEncoder) Encode(v ...interface{}) (string, error) {
	var buf bytes.Buffer
	for _, v := range v {
		if _, err := fmt.Fprintf(&buf, "%s\n", v); err != nil {
			return "", err
		}
	}
	return buf.String(), nil
}
