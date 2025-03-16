package entities

import "encoding/json"

// MCPRequest defines the structure of an MCP request
type MCPRequest struct {
	ClientID string          `json:"client_id"`
	Method   string          `json:"method"`
	Params   json.RawMessage `json:"params"`
}

// ExecuteQueryParams defines the parameters for an execute_query request
type ExecuteQueryParams struct {
	SQL string `json:"sql"`
}

// InsertDataParams defines the parameters for an insert_data request
type InsertDataParams struct {
	Table string                 `json:"table"`
	Data  map[string]interface{} `json:"data"`
}

// UpdateDataParams defines the parameters for an update_data request
type UpdateDataParams struct {
	Table     string                 `json:"table"`
	Data      map[string]interface{} `json:"data"`
	Condition string                 `json:"condition"`
}

// DeleteDataParams defines the parameters for a delete_data request
type DeleteDataParams struct {
	Table     string `json:"table"`
	Condition string `json:"condition"`
}
