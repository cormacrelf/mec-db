package api

import (
	"fmt"
	"errors"
)

const (
	// Error codes (from http package, repeated for convenience)
	// Deleted codes are ones we cannot hope to know when to return

	StatusOK                      = 200 // GET
	StatusNoContent               = 204 // PUT and POST
	StatusMultipleChoices         = 300 // GET siblings
	StatusBadRequest              = 400 // Malformed: no client id, etc
	StatusNotFound                = 404 // GET non-existent key
	StatusMethodNotAllowed        = 405 // 
	StatusNotAcceptable           = 406 // Content-Type mismatch
	StatusRequestTimeout          = 408 // Global timeout
	StatusConflict                = 409 // Unable to resolve siblings into 300
	StatusTeapot                  = 418 // Teapot is for any occasion
	StatusInternalServerError     = 500 // Any other error, eg DB
	StatusNotImplemented          = 501 // Stubs
	StatusBadGateway              = 502 // All unspecified errors from upstream
	StatusServiceUnavailable      = 503 // Server overload
	StatusGatewayTimeout          = 504 // timed out on other nodes
)

// The serializable Error structure.
type Error struct {
	error
	Code int
}

func (e *Error) Error() string {
	return fmt.Sprintf("[%d] %s", e.Code, e.Error())
}

// Codify augments an error instance with the specified code
func Codify(code int, err error) *Error {
	return &Error{
		err,
		code,
	}
}

// New creates a new Error instance with the code and message
func New(code int, msg string) *Error {
	return &Error{
		errors.New(msg),
		code,
	}
}
