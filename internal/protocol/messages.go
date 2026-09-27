// Package protocol implements the framed JSON RPC wire protocol.
package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

type Code string

const (
	InvalidRequest    Code = "INVALID_REQUEST"
	InvalidArgument   Code = "INVALID_ARGUMENT"
	MethodNotFound    Code = "METHOD_NOT_FOUND"
	Overloaded        Code = "OVERLOADED"
	ResourceExhausted Code = "RESOURCE_EXHAUSTED"
	InternalError     Code = "INTERNAL_ERROR"
	NumericError      Code = "NUMERIC_ERROR"
)

// RPCError represents a remote application error, not a transport failure.
type RPCError struct {
	Code    Code   `json:"code"`
	Message string `json:"message"`
}

func (e *RPCError) Error() string { return string(e.Code) + ": " + e.Message }

var ErrInvalidMessage = errors.New("invalid RPC message")

type Request struct {
	ID     string          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type Response struct {
	ID     string          `json:"id"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *RPCError       `json:"error,omitempty"`
}

func invalid(message string) error { return fmt.Errorf("%w: %s", ErrInvalidMessage, message) }

func (r Request) Validate() error {
	if strings.TrimSpace(r.ID) == "" {
		return invalid("id must be a nonempty string")
	}
	if strings.TrimSpace(r.Method) == "" {
		return invalid("method must be a nonempty string")
	}
	if _, err := object(r.Params); err != nil {
		return invalid("params must be a JSON object")
	}
	return nil
}

func (r Response) Validate() error {
	if strings.TrimSpace(r.ID) == "" {
		return invalid("id must be a nonempty string")
	}
	if (len(r.Result) > 0) == (r.Error != nil) {
		return invalid("exactly one of result or error is required")
	}
	if len(r.Result) > 0 && !json.Valid(r.Result) {
		return invalid("result must be valid JSON")
	}
	if r.Error != nil && (strings.TrimSpace(string(r.Error.Code)) == "" || strings.TrimSpace(r.Error.Message) == "") {
		return invalid("error requires nonempty code and message")
	}
	return nil
}

// object rejects duplicate fields to avoid ambiguous IDs and envelope shapes.
func object(data []byte) (map[string]json.RawMessage, error) {
	d := json.NewDecoder(bytes.NewReader(data))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return nil, invalid("expected JSON object")
	}
	fields := make(map[string]json.RawMessage)
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return nil, invalid("malformed object")
		}
		key, ok := token.(string)
		if !ok {
			return nil, invalid("invalid field name")
		}
		if _, exists := fields[key]; exists {
			return nil, invalid("duplicate field: " + key)
		}
		var value json.RawMessage
		if err := d.Decode(&value); err != nil {
			return nil, invalid("malformed field: " + key)
		}
		fields[key] = value
	}
	if _, err := d.Token(); err != nil {
		return nil, invalid("unterminated object")
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, invalid("trailing JSON data")
	}
	return fields, nil
}

func decodeRequest(data []byte) (Request, error) {
	var r Request
	fields, err := object(data)
	if err != nil {
		return r, err
	}
	// Decode ID first so a valid ID survives an invalid method or params field.
	if err := json.Unmarshal(fields["id"], &r.ID); err != nil || strings.TrimSpace(r.ID) == "" {
		return Request{}, invalid("id must be a nonempty string")
	}
	if err := json.Unmarshal(fields["method"], &r.Method); err != nil {
		return r, invalid("method must be a string")
	}
	r.Params = fields["params"]
	return r, r.Validate()
}

func decodeResponse(data []byte) (Response, error) {
	var r Response
	fields, err := object(data)
	if err != nil {
		return r, err
	}
	_, resultPresent := fields["result"]
	errorData, errorPresent := fields["error"]
	if resultPresent == errorPresent {
		return r, invalid("exactly one of result or error is required")
	}
	if errorPresent {
		if _, err := object(errorData); err != nil {
			return r, invalid("error must be an object")
		}
	}
	if err := json.Unmarshal(data, &r); err != nil {
		return r, invalid("invalid response field type")
	}
	return r, r.Validate()
}
