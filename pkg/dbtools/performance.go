package dbtools

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/FreePeak/db-mcp-server/internal/logger"
	"github.com/FreePeak/db-mcp-server/pkg/db"
	"github.com/FreePeak/db-mcp-server/pkg/tools"
)

// QueryMetrics stores performance metrics for a database query
type QueryMetrics struct {
	Query         string        // SQL query text
	Count         int           // Number of times the query was executed
	TotalDuration time.Duration // Total execution time
	MinDuration   time.Duration // Minimum execution time
	MaxDuration   time.Duration // Maximum execution time
	AvgDuration   time.Duration // Average execution time
	LastExecuted  time.Time     // When the query was last executed
}

// PerformanceAnalyzer tracks and analyzes database query performance
type PerformanceAnalyzer struct {
	metrics       map[string]*QueryMetrics // Map of query metrics keyed by normalized query string
	slowThreshold time.Duration            // Threshold for identifying slow queries (default: 500ms)
	mutex         sync.RWMutex             // Mutex for thread-safe access to metrics
	enabled       bool                     // Whether performance analysis is enabled
}

// NewPerformanceAnalyzer creates a new performance analyzer with default settings
func NewPerformanceAnalyzer() *PerformanceAnalyzer {
	return &PerformanceAnalyzer{
		metrics:       make(map[string]*QueryMetrics),
		slowThreshold: 500 * time.Millisecond,
		enabled:       true,
	}
}

// TrackQuery wraps a database query execution to track its performance
func (pa *PerformanceAnalyzer) TrackQuery(ctx context.Context, query string, params []interface{}, fn func() (interface{}, error)) (interface{}, error) {
	if !pa.enabled {
		return fn()
	}

	// Start timing
	startTime := time.Now()

	// Execute the query
	result, err := fn()

	// Calculate duration
	duration := time.Since(startTime)

	// Log slow queries immediately
	if duration >= pa.slowThreshold {
		paramStr := formatParams(params)
		logger.Warn("Slow query detected (%.2fms): %s [params: %s]",
			float64(duration.Milliseconds()), query, paramStr)
	}

	// Update metrics asynchronously to avoid performance impact
	go pa.updateMetrics(query, duration)

	return result, err
}

// updateMetrics updates the performance metrics for a query
func (pa *PerformanceAnalyzer) updateMetrics(query string, duration time.Duration) {
	// Normalize the query by removing specific parameter values
	normalizedQuery := normalizeQuery(query)

	pa.mutex.Lock()
	defer pa.mutex.Unlock()

	// Get or create metrics for this query
	metrics, ok := pa.metrics[normalizedQuery]
	if !ok {
		metrics = &QueryMetrics{
			Query:        query,
			MinDuration:  duration,
			MaxDuration:  duration,
			LastExecuted: time.Now(),
		}
		pa.metrics[normalizedQuery] = metrics
	}

	// Update metrics
	metrics.Count++
	metrics.TotalDuration += duration
	metrics.AvgDuration = metrics.TotalDuration / time.Duration(metrics.Count)
	metrics.LastExecuted = time.Now()

	if duration < metrics.MinDuration {
		metrics.MinDuration = duration
	}
	if duration > metrics.MaxDuration {
		metrics.MaxDuration = duration
	}
}

// GetSlowQueries returns the list of slow queries that exceed the threshold
func (pa *PerformanceAnalyzer) GetSlowQueries() []*QueryMetrics {
	pa.mutex.RLock()
	defer pa.mutex.RUnlock()

	var slowQueries []*QueryMetrics
	for _, metrics := range pa.metrics {
		if metrics.AvgDuration >= pa.slowThreshold {
			slowQueries = append(slowQueries, metrics)
		}
	}

	// Sort by average duration (slowest first)
	sort.Slice(slowQueries, func(i, j int) bool {
		return slowQueries[i].AvgDuration > slowQueries[j].AvgDuration
	})

	return slowQueries
}

// SetSlowThreshold sets the threshold for identifying slow queries
func (pa *PerformanceAnalyzer) SetSlowThreshold(threshold time.Duration) {
	pa.mutex.Lock()
	defer pa.mutex.Unlock()
	pa.slowThreshold = threshold
}

// Enable enables performance analysis
func (pa *PerformanceAnalyzer) Enable() {
	pa.mutex.Lock()
	defer pa.mutex.Unlock()
	pa.enabled = true
}

// Disable disables performance analysis
func (pa *PerformanceAnalyzer) Disable() {
	pa.mutex.Lock()
	defer pa.mutex.Unlock()
	pa.enabled = false
}

// Reset clears all collected metrics
func (pa *PerformanceAnalyzer) Reset() {
	pa.mutex.Lock()
	defer pa.mutex.Unlock()
	pa.metrics = make(map[string]*QueryMetrics)
}

// GetAllMetrics returns all collected query metrics sorted by average duration
func (pa *PerformanceAnalyzer) GetAllMetrics() []*QueryMetrics {
	pa.mutex.RLock()
	defer pa.mutex.RUnlock()

	metrics := make([]*QueryMetrics, 0, len(pa.metrics))
	for _, m := range pa.metrics {
		metrics = append(metrics, m)
	}

	// Sort by average duration (slowest first)
	sort.Slice(metrics, func(i, j int) bool {
		return metrics[i].AvgDuration > metrics[j].AvgDuration
	})

	return metrics
}

// normalizeQuery removes specific parameter values from a query for grouping similar queries
func normalizeQuery(query string) string {
	// Simplistic normalization - replace numbers and quoted strings with placeholders
	// In a real-world scenario, use a more sophisticated SQL parser
	normalized := query

	// Replace quoted strings with placeholders
	normalized = replaceRegex(normalized, `'[^']*'`, "'?'")
	normalized = replaceRegex(normalized, `"[^"]*"`, "\"?\"")

	// Replace numbers with placeholders
	normalized = replaceRegex(normalized, `\b\d+\b`, "?")

	// Remove extra whitespace
	normalized = replaceRegex(normalized, `\s+`, " ")

	return strings.TrimSpace(normalized)
}

// replaceRegex is a simple helper to replace regex matches
func replaceRegex(input, pattern, replacement string) string {
	// Use the regexp package for proper regex handling
	re, err := regexp.Compile(pattern)
	if err != nil {
		// If there's an error with the regex, just return the input
		logger.Error("Error compiling regex pattern '%s': %v", pattern, err)
		return input
	}

	return re.ReplaceAllString(input, replacement)
}

// formatParams formats query parameters for logging
func formatParams(params []interface{}) string {
	if len(params) == 0 {
		return "none"
	}

	parts := make([]string, len(params))
	for i, param := range params {
		parts[i] = fmt.Sprintf("%v", param)
	}

	return strings.Join(parts, ", ")
}

// AnalyzeQuery provides optimization suggestions for a given query
func AnalyzeQuery(query string) []string {
	suggestions := []string{}

	// Check for SELECT *
	if strings.Contains(strings.ToUpper(query), "SELECT *") {
		suggestions = append(suggestions, "Avoid using SELECT * - specify only the columns you need")
	}

	// Check for missing WHERE clause in non-aggregate queries
	if strings.Contains(strings.ToUpper(query), "SELECT") &&
		!strings.Contains(strings.ToUpper(query), "WHERE") &&
		!strings.Contains(strings.ToUpper(query), "GROUP BY") {
		suggestions = append(suggestions, "Consider adding a WHERE clause to limit the result set")
	}

	// Check for potential JOINs without conditions
	if strings.Contains(strings.ToUpper(query), "JOIN") &&
		!strings.Contains(strings.ToUpper(query), "ON") &&
		!strings.Contains(strings.ToUpper(query), "USING") {
		suggestions = append(suggestions, "Ensure all JOINs have proper conditions")
	}

	// Check for ORDER BY on non-indexed columns (simplified check)
	if strings.Contains(strings.ToUpper(query), "ORDER BY") {
		suggestions = append(suggestions, "Verify that ORDER BY columns are properly indexed")
	}

	// Check for potential subqueries that could be joins
	if strings.Contains(strings.ToUpper(query), "SELECT") &&
		strings.Contains(strings.ToUpper(query), "IN (SELECT") {
		suggestions = append(suggestions, "Consider replacing subqueries with JOINs where possible")
	}

	// Add generic suggestions if none found
	if len(suggestions) == 0 {
		suggestions = append(suggestions,
			"Consider adding appropriate indexes for frequently queried columns",
			"Review query execution plan with EXPLAIN to identify bottlenecks")
	}

	return suggestions
}

// Global instance of the performance analyzer
var performanceAnalyzer *PerformanceAnalyzer

// InitPerformanceAnalyzer initializes the global performance analyzer
func InitPerformanceAnalyzer() {
	performanceAnalyzer = NewPerformanceAnalyzer()
}

// GetPerformanceAnalyzer returns the global performance analyzer instance
func GetPerformanceAnalyzer() *PerformanceAnalyzer {
	if performanceAnalyzer == nil {
		InitPerformanceAnalyzer()
	}
	return performanceAnalyzer
}

// createPerformanceAnalyzerTool creates a tool for analyzing database performance
//
//nolint:unused // Retained for future use
func createPerformanceAnalyzerTool() *tools.Tool {
	return &tools.Tool{
		Name:        "dbPerformanceAnalyzer",
		Description: "Identify slow queries and optimization opportunities",
		Category:    "database",
		InputSchema: tools.ToolInputSchema{
			Type: "object",
			Properties: map[string]interface{}{
				"action": map[string]interface{}{
					"type":        "string",
					"description": "Action to perform (getSlowQueries, getMetrics, analyzeQuery, reset, setThreshold)",
					"enum":        []string{"getSlowQueries", "getMetrics", "analyzeQuery", "reset", "setThreshold"},
				},
				"query": map[string]interface{}{
					"type":        "string",
					"description": "SQL query to analyze",
				},
				"threshold": map[string]interface{}{
					"type":        "integer",
					"description": "Threshold in milliseconds for identifying slow queries (required for setThreshold action)",
				},
				"limit": map[string]interface{}{
					"type":        "integer",
					"description": "Maximum number of results to return (default: 10)",
				},
				"database": map[string]interface{}{
					"type":        "string",
					"description": "Database ID to use (optional if only one database is configured)",
				},
			},
			Required: []string{"query", "database"},
		},
		Handler: handlePerformanceAnalyzer,
	}
}

// handlePerformanceAnalyzer handles the performance analyzer tool execution
func handlePerformanceAnalyzer(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	// Check if database manager is initialized
	if dbManager == nil {
		return nil, fmt.Errorf("database manager not initialized")
	}

	// Extract parameters
	action, ok := getStringParam(params, "action")
	if !ok {
		return nil, fmt.Errorf("action parameter is required")
	}

	// Get database ID
	databaseID, ok := getStringParam(params, "database")
	if !ok {
		return nil, fmt.Errorf("database parameter is required")
	}

	// Get database instance
	db, err := dbManager.GetDB(databaseID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database: %w", err)
	}

	// Extract optional parameters
	query, _ := getStringParam(params, "query")
	threshold, _ := getIntParam(params, "threshold")
	limit, hasLimit := getIntParam(params, "limit")
	if !hasLimit {
		limit = 10 // Default limit
	}

	// Create context with timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Execute requested action
	switch action {
	case "getSlowQueries":
		return getSlowQueries(timeoutCtx, db, limit)
	case "getMetrics":
		return getMetrics(timeoutCtx, db)
	case "analyzeQuery":
		if query == "" {
			return nil, fmt.Errorf("query parameter is required for analyzeQuery action")
		}
		return analyzeQuery(timeoutCtx, db, query)
	case "reset":
		return resetPerformanceStats(timeoutCtx, db)
	case "setThreshold":
		if threshold <= 0 {
			return nil, fmt.Errorf("threshold parameter must be a positive integer")
		}
		return setSlowQueryThreshold(timeoutCtx, db, threshold)
	default:
		return nil, fmt.Errorf("invalid action: %s", action)
	}
}

// getSlowQueries retrieves slow queries from the database
func getSlowQueries(ctx context.Context, db db.Database, limit int) (interface{}, error) {
	query := `
		SELECT query, calls, total_time, mean_time, rows
		FROM pg_stat_statements
		WHERE total_time > 0
		ORDER BY mean_time DESC
		LIMIT $1
	`
	rows, err := db.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get slow queries: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Printf("error closing rows: %v", closeErr)
		}
	}()

	results, err := rowsToMaps(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to process slow queries: %w", err)
	}

	return map[string]interface{}{
		"slow_queries": results,
		"count":        len(results),
	}, nil
}

// getMetrics retrieves database performance metrics
func getMetrics(ctx context.Context, db db.Database) (interface{}, error) {
	query := `
		SELECT * FROM pg_stat_database
		WHERE datname = current_database()
	`
	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get metrics: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Printf("error closing rows: %v", closeErr)
		}
	}()

	results, err := rowsToMaps(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to process metrics: %w", err)
	}

	return map[string]interface{}{
		"metrics": results[0], // Only one row for current database
	}, nil
}

// analyzeQuery analyzes a specific query for performance
func analyzeQuery(ctx context.Context, db db.Database, query string) (interface{}, error) {
	explainQuery := "EXPLAIN (FORMAT JSON, ANALYZE, BUFFERS) " + query
	rows, err := db.Query(ctx, explainQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze query: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("error closing rows: %v", err)
		}
	}()

	var plan []byte
	if !rows.Next() {
		return nil, fmt.Errorf("no explain plan returned")
	}
	if err := rows.Scan(&plan); err != nil {
		return nil, fmt.Errorf("failed to scan explain plan: %w", err)
	}

	return map[string]interface{}{
		"query": query,
		"plan":  string(plan),
	}, nil
}

// resetPerformanceStats resets performance statistics
func resetPerformanceStats(ctx context.Context, db db.Database) (interface{}, error) {
	_, err := db.Exec(ctx, "SELECT pg_stat_reset()")
	if err != nil {
		return nil, fmt.Errorf("failed to reset performance stats: %w", err)
	}

	return map[string]interface{}{
		"status":  "success",
		"message": "Performance statistics have been reset",
	}, nil
}

// setSlowQueryThreshold sets the threshold for slow query logging
func setSlowQueryThreshold(ctx context.Context, db db.Database, threshold int) (interface{}, error) {
	_, err := db.Exec(ctx, "ALTER SYSTEM SET log_min_duration_statement = $1", threshold)
	if err != nil {
		return nil, fmt.Errorf("failed to set slow query threshold: %w", err)
	}

	return map[string]interface{}{
		"status":  "success",
		"message": fmt.Sprintf("Slow query threshold set to %d ms", threshold),
	}, nil
}
