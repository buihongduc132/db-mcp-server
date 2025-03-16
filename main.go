package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql" // MySQL driver
)

// Client represents an SSE connection with subscriptions
type Client struct {
	id               string
	subscribedTables []string
	eventChan        chan string
}

// MCPRequest defines the structure of an MCP request
type MCPRequest struct {
	ClientID string          `json:"client_id"`
	Method   string          `json:"method"`
	Params   json.RawMessage `json:"params"`
}

// Global variables
var (
	db      *sql.DB
	clients = make(map[string]*Client)
	mutex   = sync.Mutex{}
)

func main() {
	// Initialize MySQL connection
	// Replace with your MySQL credentials
	var err error
	db, err = sql.Open("mysql", "user:password@tcp(localhost:3306)/dbname")
	if err != nil {
		log.Fatal("Failed to connect to MySQL:", err)
	}
	defer db.Close()

	// Register HTTP handlers
	http.HandleFunc("/events", eventsHandler)
	http.HandleFunc("/execute", executeHandler)

	// Start server on port 8080
	log.Println("Server starting on :9090")
	log.Fatal(http.ListenAndServe(":9090", nil))
}

// eventsHandler handles SSE connections
func eventsHandler(w http.ResponseWriter, r *http.Request) {
	// Extract client_id and subscriptions from query parameters
	clientID := r.URL.Query().Get("client_id")
	subscribe := r.URL.Query().Get("subscribe")
	if clientID == "" || subscribe == "" {
		http.Error(w, "Missing client_id or subscribe", http.StatusBadRequest)
		return
	}

	// Parse subscribed tables
	subscribedTables := strings.Split(subscribe, ",")
	eventChan := make(chan string)
	client := &Client{
		id:               clientID,
		subscribedTables: subscribedTables,
		eventChan:        eventChan,
	}

	// Register client
	mutex.Lock()
	clients[clientID] = client
	mutex.Unlock()

	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Keep connection alive and send events
	for {
		select {
		case event := <-eventChan:
			fmt.Fprintf(w, "data: %s\n\n", event)
			w.(http.Flusher).Flush()
		case <-r.Context().Done():
			// Client disconnected
			mutex.Lock()
			delete(clients, clientID)
			mutex.Unlock()
			return
		}
	}
}

// executeHandler processes MCP method calls
func executeHandler(w http.ResponseWriter, r *http.Request) {
	var req MCPRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.ClientID == "" {
		http.Error(w, "Missing client_id", http.StatusBadRequest)
		return
	}

	// Find client
	mutex.Lock()
	client, ok := clients[req.ClientID]
	mutex.Unlock()
	if !ok {
		http.Error(w, "Client not found", http.StatusNotFound)
		return
	}

	switch req.Method {
	case "execute_query":
		var params struct {
			SQL string `json:"sql"`
		}
		err := json.Unmarshal(req.Params, &params)
		if err != nil {
			client.eventChan <- fmt.Sprintf(`{"error": "Invalid params: %s"}`, err.Error())
			return
		}
		rows, err := db.Query(params.SQL)
		if err != nil {
			client.eventChan <- fmt.Sprintf(`{"error": "%s"}`, err.Error())
			return
		}
		defer rows.Close()

		columns, err := rows.Columns()
		if err != nil {
			client.eventChan <- fmt.Sprintf(`{"error": "%s"}`, err.Error())
			return
		}

		for rows.Next() {
			values := make([]interface{}, len(columns))
			for i := range values {
				values[i] = new(interface{})
			}
			err = rows.Scan(values...)
			if err != nil {
				log.Println("Error scanning row:", err)
				continue
			}

			rowMap := make(map[string]interface{})
			for i, col := range columns {
				val := values[i].(*interface{})
				rowMap[col] = *val
			}
			jsonData, _ := json.Marshal(rowMap)
			client.eventChan <- string(jsonData)
		}
		client.eventChan <- `{"done": true}`

	case "insert_data":
		var params struct {
			Table string                 `json:"table"`
			Data  map[string]interface{} `json:"data"`
		}
		err := json.Unmarshal(req.Params, &params)
		if err != nil {
			client.eventChan <- fmt.Sprintf(`{"error": "Invalid params: %s"}`, err.Error())
			return
		}
		columns := []string{}
		placeholders := []string{}
		values := []interface{}{}
		for col, val := range params.Data {
			columns = append(columns, col)
			placeholders = append(placeholders, "?")
			values = append(values, val)
		}
		sqlStr := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", params.Table, strings.Join(columns, ","), strings.Join(placeholders, ","))
		result, err := db.Exec(sqlStr, values...)
		if err != nil {
			client.eventChan <- fmt.Sprintf(`{"error": "%s"}`, err.Error())
			return
		}
		id, _ := result.LastInsertId()
		client.eventChan <- fmt.Sprintf(`{"inserted_id": %d}`, id)

		// Notify subscribed clients
		dataJSON, _ := json.Marshal(params.Data)
		changeEvent := fmt.Sprintf(`{"event": "insert", "table": "%s", "data": %s}`, params.Table, string(dataJSON))
		sendChangeEvent(params.Table, changeEvent)

	case "update_data":
		var params struct {
			Table     string                 `json:"table"`
			Data      map[string]interface{} `json:"data"`
			Condition string                 `json:"condition"`
		}
		err := json.Unmarshal(req.Params, &params)
		if err != nil {
			client.eventChan <- fmt.Sprintf(`{"error": "Invalid params: %s"}`, err.Error())
			return
		}
		sets := []string{}
		values := []interface{}{}
		for col, val := range params.Data {
			sets = append(sets, fmt.Sprintf("%s = ?", col))
			values = append(values, val)
		}
		sqlStr := fmt.Sprintf("UPDATE %s SET %s WHERE %s", params.Table, strings.Join(sets, ","), params.Condition)
		result, err := db.Exec(sqlStr, values...)
		if err != nil {
			client.eventChan <- fmt.Sprintf(`{"error": "%s"}`, err.Error())
			return
		}
		rowsAffected, _ := result.RowsAffected()
		client.eventChan <- fmt.Sprintf(`{"rows_affected": %d}`, rowsAffected)

		// Notify subscribed clients
		dataJSON, _ := json.Marshal(params.Data)
		changeEvent := fmt.Sprintf(`{"event": "update", "table": "%s", "condition": "%s", "data": %s}`, params.Table, params.Condition, string(dataJSON))
		sendChangeEvent(params.Table, changeEvent)

	case "delete_data":
		var params struct {
			Table     string `json:"table"`
			Condition string `json:"condition"`
		}
		err := json.Unmarshal(req.Params, &params)
		if err != nil {
			client.eventChan <- fmt.Sprintf(`{"error": "Invalid params: %s"}`, err.Error())
			return
		}
		sqlStr := fmt.Sprintf("DELETE FROM %s WHERE %s", params.Table, params.Condition)
		result, err := db.Exec(sqlStr)
		if err != nil {
			client.eventChan <- fmt.Sprintf(`{"error": "%s"}`, err.Error())
			return
		}
		rowsAffected, _ := result.RowsAffected()
		client.eventChan <- fmt.Sprintf(`{"rows_affected": %d}`, rowsAffected)

		// Notify subscribed clients
		changeEvent := fmt.Sprintf(`{"event": "delete", "table": "%s", "condition": "%s"}`, params.Table, params.Condition)
		sendChangeEvent(params.Table, changeEvent)

	default:
		http.Error(w, "Unknown method", http.StatusBadRequest)
	}
}

// sendChangeEvent notifies subscribed clients of table changes
func sendChangeEvent(table, event string) {
	mutex.Lock()
	defer mutex.Unlock()
	for _, client := range clients {
		for _, subTable := range client.subscribedTables {
			if subTable == table {
				client.eventChan <- event
				break
			}
		}
	}
}
