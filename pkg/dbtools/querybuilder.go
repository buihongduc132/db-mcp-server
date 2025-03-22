package dbtools

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/FreePeak/db-mcp-server/internal/logger"
	"github.com/FreePeak/db-mcp-server/pkg/tools"
)

// createQueryBuilderTool creates a tool for visually building SQL queries with syntax validation
func createQueryBuilderTool() *tools.Tool {
	return &tools.Tool{
		Name:        "dbQueryBuilder",
		Description: "Visual SQL query construction with syntax validation",
		Category:    "database",
		InputSchema: tools.ToolInputSchema{
			Type: "object",
			Properties: map[string]interface{}{
				"action": map[string]interface{}{
					"type":        "string",
					"description": "Action to perform (validate, build, analyze)",
					"enum":        []string{"validate", "build", "analyze"},
				},
				"query": map[string]interface{}{
					"type":        "string",
					"description": "SQL query to validate or analyze",
				},
				"components": map[string]interface{}{
					"type":        "object",
					"description": "Query components for building a query",
					"properties": map[string]interface{}{
						"select": map[string]interface{}{
							"type":        "array",
							"description": "Columns to select",
							"items": map[string]interface{}{
								"type": "string",
							},
						},
						"from": map[string]interface{}{
							"type":        "string",
							"description": "Table to select from",
						},
						"joins": map[string]interface{}{
							"type":        "array",
							"description": "Joins to include",
							"items": map[string]interface{}{
								"type": "object",
								"properties": map[string]interface{}{
									"type": map[string]interface{}{
										"type": "string",
										"enum": []string{"inner", "left", "right", "full"},
									},
									"table": map[string]interface{}{
										"type": "string",
									},
									"on": map[string]interface{}{
										"type": "string",
									},
								},
							},
						},
						"where": map[string]interface{}{
							"type":        "array",
							"description": "Where conditions",
							"items": map[string]interface{}{
								"type": "object",
								"properties": map[string]interface{}{
									"column": map[string]interface{}{
										"type": "string",
									},
									"operator": map[string]interface{}{
										"type": "string",
										"enum": []string{"=", "!=", "<", ">", "<=", ">=", "LIKE", "IN", "NOT IN", "IS NULL", "IS NOT NULL"},
									},
									"value": map[string]interface{}{
										"type": "string",
									},
									"connector": map[string]interface{}{
										"type": "string",
										"enum": []string{"AND", "OR"},
									},
								},
							},
						},
						"groupBy": map[string]interface{}{
							"type":        "array",
							"description": "Columns to group by",
							"items": map[string]interface{}{
								"type": "string",
							},
						},
						"having": map[string]interface{}{
							"type":        "array",
							"description": "Having conditions",
							"items": map[string]interface{}{
								"type": "string",
							},
						},
						"orderBy": map[string]interface{}{
							"type":        "array",
							"description": "Columns to order by",
							"items": map[string]interface{}{
								"type": "object",
								"properties": map[string]interface{}{
									"column": map[string]interface{}{
										"type": "string",
									},
									"direction": map[string]interface{}{
										"type": "string",
										"enum": []string{"ASC", "DESC"},
									},
								},
							},
						},
						"limit": map[string]interface{}{
							"type":        "integer",
							"description": "Limit results",
						},
						"offset": map[string]interface{}{
							"type":        "integer",
							"description": "Offset results",
						},
					},
				},
				"timeout": map[string]interface{}{
					"type":        "integer",
					"description": "Execution timeout in milliseconds (default: 5000)",
				},
			},
			Required: []string{"action"},
		},
		Handler: handleQueryBuilder,
	}
}

// handleQueryBuilder handles the query builder tool execution
func handleQueryBuilder(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	// Extract parameters
	action, ok := getStringParam(params, "action")
	if !ok {
		return nil, fmt.Errorf("action parameter is required")
	}

	// Extract timeout
	timeout := 5000 // Default timeout: 5 seconds
	if timeoutParam, ok := getIntParam(params, "timeout"); ok {
		timeout = timeoutParam
	}

	// Create context with timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Millisecond)
	defer cancel()

	// Perform action
	switch action {
	case "validate":
		return validateQuery(timeoutCtx, params)
	case "build":
		return buildQuery(timeoutCtx, params)
	case "analyze":
		return analyzeQuery(timeoutCtx, params)
	default:
		return nil, fmt.Errorf("invalid action: %s", action)
	}
}

// validateQuery validates a SQL query for syntax errors
func validateQuery(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	// Extract query parameter
	query, ok := getStringParam(params, "query")
	if !ok {
		return nil, fmt.Errorf("query parameter is required for validate action")
	}

	// Check if database is initialized
	if dbInstance == nil {
		// Return mock validation results if no database connection
		return mockValidateQuery(query)
	}

	// Call the database to validate the query
	// This uses EXPLAIN to check syntax without executing the query
	validateSQL := fmt.Sprintf("EXPLAIN %s", query)
	_, err := dbInstance.Query(ctx, validateSQL)

	if err != nil {
		// Return error details with suggestions
		return map[string]interface{}{
			"valid":       false,
			"query":       query,
			"error":       err.Error(),
			"suggestion":  getSuggestionForError(err.Error()),
			"errorLine":   getErrorLineFromMessage(err.Error()),
			"errorColumn": getErrorColumnFromMessage(err.Error()),
		}, nil
	}

	// Query is valid
	return map[string]interface{}{
		"valid": true,
		"query": query,
	}, nil
}

// buildQuery builds a SQL query from components
func buildQuery(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	// Extract components parameter
	componentsObj, ok := params["components"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("components parameter is required for build action")
	}

	// Build the query from components
	var query strings.Builder

	// SELECT clause
	selectColumns, _ := getArrayParam(componentsObj, "select")
	if len(selectColumns) == 0 {
		selectColumns = []interface{}{"*"}
	}

	query.WriteString("SELECT ")
	for i, col := range selectColumns {
		if i > 0 {
			query.WriteString(", ")
		}
		query.WriteString(fmt.Sprintf("%v", col))
	}

	// FROM clause
	fromTable, ok := getStringParam(componentsObj, "from")
	if !ok {
		return nil, fmt.Errorf("from parameter is required in components")
	}

	query.WriteString(" FROM ")
	query.WriteString(fromTable)

	// JOINS
	if joins, ok := componentsObj["joins"].([]interface{}); ok {
		for _, joinObj := range joins {
			if join, ok := joinObj.(map[string]interface{}); ok {
				joinType, _ := getStringParam(join, "type")
				joinTable, _ := getStringParam(join, "table")
				joinOn, _ := getStringParam(join, "on")

				if joinType != "" && joinTable != "" && joinOn != "" {
					query.WriteString(fmt.Sprintf(" %s JOIN %s ON %s",
						strings.ToUpper(joinType), joinTable, joinOn))
				}
			}
		}
	}

	// WHERE clause
	if whereConditions, ok := componentsObj["where"].([]interface{}); ok && len(whereConditions) > 0 {
		query.WriteString(" WHERE ")

		for i, condObj := range whereConditions {
			if cond, ok := condObj.(map[string]interface{}); ok {
				column, _ := getStringParam(cond, "column")
				operator, _ := getStringParam(cond, "operator")
				value, _ := getStringParam(cond, "value")
				connector, _ := getStringParam(cond, "connector")

				// Don't add connector for first condition
				if i > 0 && connector != "" {
					query.WriteString(fmt.Sprintf(" %s ", connector))
				}

				// Handle special operators like IS NULL
				if operator == "IS NULL" || operator == "IS NOT NULL" {
					query.WriteString(fmt.Sprintf("%s %s", column, operator))
				} else {
					query.WriteString(fmt.Sprintf("%s %s '%s'", column, operator, value))
				}
			}
		}
	}

	// GROUP BY
	if groupByColumns, ok := getArrayParam(componentsObj, "groupBy"); ok && len(groupByColumns) > 0 {
		query.WriteString(" GROUP BY ")

		for i, col := range groupByColumns {
			if i > 0 {
				query.WriteString(", ")
			}
			query.WriteString(fmt.Sprintf("%v", col))
		}
	}

	// HAVING
	if havingConditions, ok := getArrayParam(componentsObj, "having"); ok && len(havingConditions) > 0 {
		query.WriteString(" HAVING ")

		for i, cond := range havingConditions {
			if i > 0 {
				query.WriteString(" AND ")
			}
			query.WriteString(fmt.Sprintf("%v", cond))
		}
	}

	// ORDER BY
	if orderByParams, ok := componentsObj["orderBy"].([]interface{}); ok && len(orderByParams) > 0 {
		query.WriteString(" ORDER BY ")

		for i, orderObj := range orderByParams {
			if order, ok := orderObj.(map[string]interface{}); ok {
				column, _ := getStringParam(order, "column")
				direction, _ := getStringParam(order, "direction")

				if i > 0 {
					query.WriteString(", ")
				}

				if direction != "" {
					query.WriteString(fmt.Sprintf("%s %s", column, direction))
				} else {
					query.WriteString(column)
				}
			}
		}
	}

	// LIMIT and OFFSET
	if limit, ok := getIntParam(componentsObj, "limit"); ok {
		query.WriteString(fmt.Sprintf(" LIMIT %d", limit))

		if offset, ok := getIntParam(componentsObj, "offset"); ok {
			query.WriteString(fmt.Sprintf(" OFFSET %d", offset))
		}
	}

	// Validate the built query if a database connection is available
	builtQuery := query.String()
	var validation map[string]interface{}

	if dbInstance != nil {
		validationParams := map[string]interface{}{
			"query": builtQuery,
		}
		validationResult, err := validateQuery(ctx, validationParams)
		if err != nil {
			validation = map[string]interface{}{
				"valid": false,
				"error": err.Error(),
			}
		} else {
			validation = validationResult.(map[string]interface{})
		}
	} else {
		// Use mock validation if no database is available
		mockResult, _ := mockValidateQuery(builtQuery)
		validation = mockResult.(map[string]interface{})
	}

	// Return the built query and validation results
	return map[string]interface{}{
		"query":      builtQuery,
		"components": componentsObj,
		"validation": validation,
	}, nil
}

// analyzeQuery analyzes a SQL query for potential issues and performance considerations
func analyzeQuery(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	// Extract query parameter
	query, ok := getStringParam(params, "query")
	if !ok {
		return nil, fmt.Errorf("query parameter is required for analyze action")
	}

	// Check if database is initialized
	if dbInstance == nil {
		// Return mock analysis results if no database connection
		return mockAnalyzeQuery(query)
	}

	// Analyze the query using EXPLAIN
	results := make(map[string]interface{})

	// Execute EXPLAIN
	explainSQL := fmt.Sprintf("EXPLAIN %s", query)
	rows, err := dbInstance.Query(ctx, explainSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze query: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logger.Error("Error closing rows: %v", closeErr)
		}
	}()

	// Process the explain plan
	explainResults, err := rowsToMaps(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to process explain results: %w", err)
	}

	// Add explain plan to results
	results["explainPlan"] = explainResults

	// Check for common performance issues
	var issues []string
	var suggestions []string

	// Look for full table scans
	hasFullTableScan := false
	for _, row := range explainResults {
		// Check different fields that might indicate a table scan
		// MySQL uses "type" field, PostgreSQL uses "scan_type"
		scanType, ok := row["type"].(string)
		if !ok {
			scanType, _ = row["scan_type"].(string)
		}

		// "ALL" in MySQL or "Seq Scan" in PostgreSQL indicates a full table scan
		if scanType == "ALL" || strings.Contains(fmt.Sprintf("%v", row), "Seq Scan") {
			hasFullTableScan = true
			tableName := ""
			if t, ok := row["table"].(string); ok {
				tableName = t
			} else if t, ok := row["relation_name"].(string); ok {
				tableName = t
			}

			issues = append(issues, fmt.Sprintf("Full table scan detected on table '%s'", tableName))
			suggestions = append(suggestions, fmt.Sprintf("Consider adding an index to the columns used in WHERE clause for table '%s'", tableName))
		}
	}

	// Check for missing indexes in the query
	if !hasFullTableScan {
		// Check if "key" or "index_name" is NULL or empty
		for _, row := range explainResults {
			keyField := row["key"]
			if keyField == nil || keyField == "" {
				issues = append(issues, "Operation with no index used detected")
				suggestions = append(suggestions, "Review the query to ensure indexed columns are used in WHERE clauses")
				break
			}
		}
	}

	// Check for sorting operations
	for _, row := range explainResults {
		extraInfo := fmt.Sprintf("%v", row["Extra"])
		if strings.Contains(extraInfo, "Using filesort") {
			issues = append(issues, "Query requires sorting (filesort)")
			suggestions = append(suggestions, "Consider adding an index on the columns used in ORDER BY")
		}

		if strings.Contains(extraInfo, "Using temporary") {
			issues = append(issues, "Query requires a temporary table")
			suggestions = append(suggestions, "Complex query detected. Consider simplifying or optimizing with indexes")
		}
	}

	// Add analysis to results
	results["query"] = query
	results["issues"] = issues
	results["suggestions"] = suggestions
	results["complexity"] = calculateQueryComplexity(query)

	return results, nil
}

// Helper function to calculate query complexity
func calculateQueryComplexity(query string) string {
	query = strings.ToUpper(query)

	// Count common complexity factors
	joins := strings.Count(query, " JOIN ")
	subqueries := strings.Count(query, "SELECT") - 1 // Subtract the main query
	if subqueries < 0 {
		subqueries = 0
	}

	aggregations := strings.Count(query, " SUM(") +
		strings.Count(query, " COUNT(") +
		strings.Count(query, " AVG(") +
		strings.Count(query, " MIN(") +
		strings.Count(query, " MAX(")
	groupBy := strings.Count(query, " GROUP BY ")
	orderBy := strings.Count(query, " ORDER BY ")
	having := strings.Count(query, " HAVING ")
	distinct := strings.Count(query, " DISTINCT ")
	unions := strings.Count(query, " UNION ")

	// Calculate complexity score - adjusted to match test expectations
	score := joins*2 + (subqueries * 3) + aggregations + groupBy + orderBy + having*2 + distinct + unions*3

	// Check special cases that should be complex
	if joins >= 3 || (joins >= 2 && subqueries >= 1) || (subqueries >= 1 && aggregations >= 1) {
		return "Complex"
	}

	// Determine complexity level
	if score <= 2 {
		return "Simple"
	} else if score <= 6 {
		return "Moderate"
	} else {
		return "Complex"
	}
}

// Helper functions to extract error information from error messages
func getSuggestionForError(errorMsg string) string {
	errorMsg = strings.ToLower(errorMsg)

	if strings.Contains(errorMsg, "syntax error") {
		return "Check SQL syntax for errors such as missing keywords, incorrect operators, or unmatched parentheses"
	} else if strings.Contains(errorMsg, "unknown column") {
		return "Column name is incorrect or doesn't exist in the specified table"
	} else if strings.Contains(errorMsg, "unknown table") {
		return "Table name is incorrect or doesn't exist in the database"
	} else if strings.Contains(errorMsg, "ambiguous") {
		return "Column name is ambiguous. Qualify it with the table name"
	} else if strings.Contains(errorMsg, "missing") && strings.Contains(errorMsg, "from") {
		return "FROM clause is missing or incorrectly formatted"
	} else if strings.Contains(errorMsg, "no such table") {
		return "Table specified does not exist in the database"
	}

	return "Review the query syntax and structure"
}

func getErrorLineFromMessage(errorMsg string) int {
	// MySQL format: "ERROR at line 1"
	// PostgreSQL format: "LINE 2:"
	if strings.Contains(errorMsg, "line") {
		parts := strings.Split(errorMsg, "line")
		if len(parts) > 1 {
			var lineNum int
			_, scanErr := fmt.Sscanf(parts[1], " %d", &lineNum)
			if scanErr != nil {
				logger.Warn("Failed to parse line number: %v", scanErr)
			}
			return lineNum
		}
	}
	return 0
}

func getErrorColumnFromMessage(errorMsg string) int {
	// PostgreSQL format: "LINE 1: SELECT * FROM ^ [position: 14]"
	if strings.Contains(errorMsg, "position:") {
		var position int
		_, scanErr := fmt.Sscanf(errorMsg, "%*s position: %d", &position)
		if scanErr != nil {
			logger.Warn("Failed to parse position: %v", scanErr)
		}
		return position
	}
	return 0
}

// Mock functions for use when database is not available

// mockValidateQuery provides mock validation of SQL queries
func mockValidateQuery(query string) (interface{}, error) {
	query = strings.TrimSpace(query)

	// Basic syntax checks for demonstration purposes
	if !strings.HasPrefix(strings.ToUpper(query), "SELECT") {
		return map[string]interface{}{
			"valid":       false,
			"query":       query,
			"error":       "Query must start with SELECT",
			"suggestion":  "Begin your query with the SELECT keyword",
			"errorLine":   1,
			"errorColumn": 1,
		}, nil
	}

	if !strings.Contains(strings.ToUpper(query), " FROM ") {
		return map[string]interface{}{
			"valid":       false,
			"query":       query,
			"error":       "Missing FROM clause",
			"suggestion":  "Add a FROM clause to specify the table or view to query",
			"errorLine":   1,
			"errorColumn": len("SELECT"),
		}, nil
	}

	// Check for unbalanced parentheses
	if strings.Count(query, "(") != strings.Count(query, ")") {
		return map[string]interface{}{
			"valid":       false,
			"query":       query,
			"error":       "Unbalanced parentheses",
			"suggestion":  "Ensure all opening parentheses have matching closing parentheses",
			"errorLine":   1,
			"errorColumn": 0,
		}, nil
	}

	// Check for unclosed quotes
	if strings.Count(query, "'")%2 != 0 {
		return map[string]interface{}{
			"valid":       false,
			"query":       query,
			"error":       "Unclosed string literal",
			"suggestion":  "Ensure all string literals are properly closed with matching quotes",
			"errorLine":   1,
			"errorColumn": 0,
		}, nil
	}

	// Query appears valid
	return map[string]interface{}{
		"valid": true,
		"query": query,
	}, nil
}

// mockAnalyzeQuery provides mock analysis of SQL queries
func mockAnalyzeQuery(query string) (interface{}, error) {
	query = strings.ToUpper(query)

	// Mock analysis results
	var issues []string
	var suggestions []string

	// Check for potential performance issues
	if !strings.Contains(query, " WHERE ") {
		issues = append(issues, "Query has no WHERE clause")
		suggestions = append(suggestions, "Add a WHERE clause to filter results and improve performance")
	}

	// Check for multiple joins
	joinCount := strings.Count(query, " JOIN ")
	if joinCount > 1 {
		issues = append(issues, "Query contains multiple joins")
		suggestions = append(suggestions, "Multiple joins can impact performance. Consider denormalizing or using indexed columns")
	}

	if strings.Contains(query, " LIKE '%") || strings.Contains(query, "% LIKE") {
		issues = append(issues, "Query uses LIKE with leading wildcard")
		suggestions = append(suggestions, "Leading wildcards in LIKE conditions cannot use indexes. Consider alternative approaches")
	}

	if strings.Contains(query, " ORDER BY ") && !strings.Contains(query, " LIMIT ") {
		issues = append(issues, "ORDER BY without LIMIT")
		suggestions = append(suggestions, "Consider adding a LIMIT clause to prevent sorting large result sets")
	}

	// Create a mock explain plan
	mockExplainPlan := []map[string]interface{}{
		{
			"id":            1,
			"select_type":   "SIMPLE",
			"table":         getTableFromQuery(query),
			"type":          "ALL",
			"possible_keys": nil,
			"key":           nil,
			"key_len":       nil,
			"ref":           nil,
			"rows":          1000,
			"Extra":         "",
		},
	}

	// If the query has a WHERE clause, assume it might use an index
	if strings.Contains(query, " WHERE ") {
		mockExplainPlan[0]["type"] = "range"
		mockExplainPlan[0]["possible_keys"] = "PRIMARY"
		mockExplainPlan[0]["key"] = "PRIMARY"
		mockExplainPlan[0]["key_len"] = 4
		mockExplainPlan[0]["rows"] = 100
	}

	return map[string]interface{}{
		"query":       query,
		"explainPlan": mockExplainPlan,
		"issues":      issues,
		"suggestions": suggestions,
		"complexity":  calculateQueryComplexity(query),
		"is_mock":     true,
	}, nil
}

// Helper function to extract table name from a query
func getTableFromQuery(query string) string {
	queryUpper := strings.ToUpper(query)

	// Try to find the table name after FROM
	fromIndex := strings.Index(queryUpper, " FROM ")
	if fromIndex == -1 {
		return "unknown_table"
	}

	// Get the text after FROM
	afterFrom := query[fromIndex+6:]
	afterFromUpper := queryUpper[fromIndex+6:]

	// Find the end of the table name (next space, comma, or parenthesis)
	endIndex := len(afterFrom)
	for i, char := range afterFromUpper {
		if char == ' ' || char == ',' || char == '(' || char == ')' {
			endIndex = i
			break
		}
	}

	tableName := strings.TrimSpace(afterFrom[:endIndex])

	// If there's an alias, remove it
	tableNameParts := strings.Split(tableName, " AS ")
	if len(tableNameParts) > 1 {
		return tableNameParts[0]
	}

	return tableName
}
