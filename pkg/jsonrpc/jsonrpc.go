package jsonrpc

import (
	"encoding/json"
	"fmt"
)

const (
	// Version is the JSON-RPC version
	Version = "2.0"
)

// Request represents a JSON-RPC request
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      interface{}     `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// IsNotification returns true if the request is a notification (no ID)
func (r *Request) IsNotification() bool {
	return r.ID == nil
}

// Response represents a JSON-RPC response
type Response struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      interface{} `json:"id,omitempty"`
	Result  interface{} `json:"result,omitempty"`
	Error   *Error      `json:"error,omitempty"`
}

// Error represents a JSON-RPC error
type Error struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// Standard error codes as defined in the JSON-RPC 2.0 spec
const (
	ParseErrorCode     = -32700
	InvalidRequestCode = -32600
	MethodNotFoundCode = -32601
	InvalidParamsCode  = -32602
	InternalErrorCode  = -32603
)

// Error returns a string representation of the error
func (e *Error) Error() string {
	return fmt.Sprintf("JSON-RPC error %d: %s", e.Code, e.Message)
}

// NewResponse creates a new response for the given request
func NewResponse(req *Request, result interface{}, err *Error) *Response {
	resp := &Response{
		JSONRPC: Version,
		ID:      req.ID,
	}

	if err != nil {
		resp.Error = err
	} else {
		resp.Result = result
	}

	return resp
}

// NewError creates a new Error with the given code and message
func NewError(code int, message string, data interface{}) *Error {
	return &Error{
		Code:    code,
		Message: message,
		Data:    data,
	}
}

// ParseError creates a parse error
func ParseError(data interface{}) *Error {
	return NewError(ParseErrorCode, "Parse error", data)
}

// InvalidRequestError creates an invalid request error
func InvalidRequestError(data interface{}) *Error {
	return NewError(InvalidRequestCode, "Invalid Request", data)
}

// MethodNotFoundError creates a method not found error
func MethodNotFoundError(data interface{}) *Error {
	return NewError(MethodNotFoundCode, "Method not found", data)
}

// InvalidParamsError creates an invalid params error
func InvalidParamsError(data interface{}) *Error {
	return NewError(InvalidParamsCode, "Invalid params", data)
}

// InternalError creates an internal error
func InternalError(data interface{}) *Error {
	return NewError(InternalErrorCode, "Internal error", data)
}
