package dbtools

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/FreePeak/db-mcp-server/internal/logger"
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
					"description": "SQL query to analyze (required for analyzeQuery action)",
				},
				"threshold": map[string]interface{}{
					"type":        "integer",
					"description": "Threshold in milliseconds for identifying slow queries (required for setThreshold action)",
				},
				"limit": map[string]interface{}{
					"type":        "integer",
					"description": "Maximum number of results to return (default: 10)",
				},
			},
			Required: []string{"action"},
		},
		Handler: handlePerformanceAnalyzer,
	}
}

// handlePerformanceAnalyzer handles the performance analyzer tool execution
func handlePerformanceAnalyzer(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	// Check if database is initialized
	if dbInstance == nil {
		return nil, fmt.Errorf("database not initialized")
	}

	// Get the performance analyzer
	analyzer := GetPerformanceAnalyzer()

	// Extract action parameter
	action, ok := getStringParam(params, "action")
	if !ok {
		return nil, fmt.Errorf("action parameter is required")
	}

	// Extract limit parameter (default: 10)
	limit := 10
	if limitParam, ok := getIntParam(params, "limit"); ok {
		limit = limitParam
	}

	// Handle different actions
	switch action {
	case "getSlowQueries":
		// Get slow queries
		slowQueries := analyzer.GetSlowQueries()

		// Apply limit
		if len(slowQueries) > limit {
			slowQueries = slowQueries[:limit]
		}

		// Convert to response format
		result := makeMetricsResponse(slowQueries)
		return result, nil

	case "getMetrics":
		// Get all metrics
		metrics := analyzer.GetAllMetrics()

		// Apply limit
		if len(metrics) > limit {
			metrics = metrics[:limit]
		}

		// Convert to response format
		result := makeMetricsResponse(metrics)
		return result, nil

	case "analyzeQuery":
		// Extract query parameter
		query, ok := getStringParam(params, "query")
		if !ok {
			return nil, fmt.Errorf("query parameter is required for analyzeQuery action")
		}

		// Analyze the query
		suggestions := AnalyzeQuery(query)

		return map[string]interface{}{
			"query":       query,
			"suggestions": suggestions,
		}, nil

	case "reset":
		// Reset metrics
		analyzer.Reset()
		return map[string]interface{}{
			"success": true,
			"message": "Performance metrics have been reset",
		}, nil

	case "setThreshold":
		// Extract threshold parameter
		thresholdMs, ok := getIntParam(params, "threshold")
		if !ok {
			return nil, fmt.Errorf("threshold parameter is required for setThreshold action")
		}

		// Set threshold
		analyzer.SetSlowThreshold(time.Duration(thresholdMs) * time.Millisecond)

		return map[string]interface{}{
			"success":   true,
			"message":   "Slow query threshold updated",
			"threshold": fmt.Sprintf("%dms", thresholdMs),
		}, nil

	default:
		return nil, fmt.Errorf("unknown action: %s", action)
	}
}

// makeMetricsResponse converts metrics to a response format
func makeMetricsResponse(metrics []*QueryMetrics) map[string]interface{} {
	queries := make([]map[string]interface{}, len(metrics))

	for i, m := range metrics {
		queries[i] = map[string]interface{}{
			"query":         m.Query,
			"count":         m.Count,
			"avgDuration":   fmt.Sprintf("%.2fms", float64(m.AvgDuration.Microseconds())/1000),
			"minDuration":   fmt.Sprintf("%.2fms", float64(m.MinDuration.Microseconds())/1000),
			"maxDuration":   fmt.Sprintf("%.2fms", float64(m.MaxDuration.Microseconds())/1000),
			"totalDuration": fmt.Sprintf("%.2fms", float64(m.TotalDuration.Microseconds())/1000),
			"lastExecuted":  m.LastExecuted.Format(time.RFC3339),
		}
	}

	return map[string]interface{}{
		"queries": queries,
		"count":   len(metrics),
	}
}
