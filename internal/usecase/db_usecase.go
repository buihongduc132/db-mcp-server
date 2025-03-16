package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"mcpserver/internal/domain/entities"
	"mcpserver/internal/domain/repositories"
)

// DBUseCase handles database-related operations
type DBUseCase struct {
	dbRepo     repositories.DBRepository
	clientCase *ClientUseCase
}

// NewDBUseCase creates a new database use case
func NewDBUseCase(dbRepo repositories.DBRepository, clientCase *ClientUseCase) *DBUseCase {
	return &DBUseCase{
		dbRepo:     dbRepo,
		clientCase: clientCase,
	}
}

// ExecuteQuery executes a SQL query and returns the results
func (uc *DBUseCase) ExecuteQuery(ctx context.Context, sql string) ([]map[string]interface{}, error) {
	return uc.dbRepo.ExecuteQuery(ctx, sql)
}

// InsertData inserts data into a table
func (uc *DBUseCase) InsertData(ctx context.Context, table string, data map[string]interface{}) (int64, error) {
	id, err := uc.dbRepo.InsertData(ctx, table, data)
	if err != nil {
		return 0, err
	}

	// Notify subscribed clients about the change
	dataJSON, _ := json.Marshal(data)
	changeEvent := fmt.Sprintf(`{"event": "insert", "table": "%s", "data": %s}`, table, string(dataJSON))
	uc.clientCase.NotifySubscribers(ctx, table, changeEvent)

	return id, nil
}

// UpdateData updates data in a table based on a condition
func (uc *DBUseCase) UpdateData(ctx context.Context, table string, data map[string]interface{}, condition string) (int64, error) {
	affected, err := uc.dbRepo.UpdateData(ctx, table, data, condition)
	if err != nil {
		return 0, err
	}

	// Notify subscribed clients about the change
	dataJSON, _ := json.Marshal(data)
	changeEvent := fmt.Sprintf(`{"event": "update", "table": "%s", "condition": "%s", "data": %s}`, table, condition, string(dataJSON))
	uc.clientCase.NotifySubscribers(ctx, table, changeEvent)

	return affected, nil
}

// DeleteData deletes data from a table based on a condition
func (uc *DBUseCase) DeleteData(ctx context.Context, table string, condition string) (int64, error) {
	affected, err := uc.dbRepo.DeleteData(ctx, table, condition)
	if err != nil {
		return 0, err
	}

	// Notify subscribed clients about the change
	changeEvent := fmt.Sprintf(`{"event": "delete", "table": "%s", "condition": "%s"}`, table, condition)
	uc.clientCase.NotifySubscribers(ctx, table, changeEvent)

	return affected, nil
}

// ProcessRequest processes an MCP request and returns the result
func (uc *DBUseCase) ProcessRequest(ctx context.Context, req *entities.MCPRequest) (interface{}, error) {
	switch req.Method {
	case "execute_query":
		var params entities.ExecuteQueryParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, fmt.Errorf("invalid params: %w", err)
		}
		return uc.ExecuteQuery(ctx, params.SQL)

	case "insert_data":
		var params entities.InsertDataParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, fmt.Errorf("invalid params: %w", err)
		}
		id, err := uc.InsertData(ctx, params.Table, params.Data)
		if err != nil {
			return nil, err
		}
		return map[string]int64{"inserted_id": id}, nil

	case "update_data":
		var params entities.UpdateDataParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, fmt.Errorf("invalid params: %w", err)
		}
		affected, err := uc.UpdateData(ctx, params.Table, params.Data, params.Condition)
		if err != nil {
			return nil, err
		}
		return map[string]int64{"rows_affected": affected}, nil

	case "delete_data":
		var params entities.DeleteDataParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, fmt.Errorf("invalid params: %w", err)
		}
		affected, err := uc.DeleteData(ctx, params.Table, params.Condition)
		if err != nil {
			return nil, err
		}
		return map[string]int64{"rows_affected": affected}, nil

	default:
		return nil, fmt.Errorf("unknown method: %s", req.Method)
	}
}
