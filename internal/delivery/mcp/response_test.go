package mcp

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

func TestNewResponse(t *testing.T) {
	resp := NewResponse()
	if resp == nil {
		t.Fatal("NewResponse returned nil")
	}
	if len(resp.Content) != 0 {
		t.Errorf("Expected empty content, got %v", resp.Content)
	}
	if resp.Metadata != nil {
		t.Errorf("Expected nil metadata, got %v", resp.Metadata)
	}
}

func TestWithText(t *testing.T) {
	resp := NewResponse().WithText("Hello, world!")
	if len(resp.Content) != 1 {
		t.Fatalf("Expected 1 content item, got %d", len(resp.Content))
	}
	if resp.Content[0].Type != "text" {
		t.Errorf("Expected content type 'text', got %s", resp.Content[0].Type)
	}
	if resp.Content[0].Text != "Hello, world!" {
		t.Errorf("Expected content text 'Hello, world!', got %s", resp.Content[0].Text)
	}

	// Test chaining multiple texts
	resp2 := resp.WithText("Second line")
	if len(resp2.Content) != 2 {
		t.Fatalf("Expected 2 content items, got %d", len(resp2.Content))
	}
	if resp2.Content[1].Text != "Second line" {
		t.Errorf("Expected second content text 'Second line', got %s", resp2.Content[1].Text)
	}
}

func TestWithMetadata(t *testing.T) {
	resp := NewResponse().WithMetadata("key", "value")
	if resp.Metadata == nil {
		t.Fatalf("Expected metadata to be initialized")
	}
	if val, ok := resp.Metadata["key"]; !ok || val != "value" {
		t.Errorf("Expected metadata['key'] = 'value', got %v", val)
	}

	// Test chaining multiple metadata
	resp2 := resp.WithMetadata("key2", 123)
	if len(resp2.Metadata) != 2 {
		t.Fatalf("Expected 2 metadata items, got %d", len(resp2.Metadata))
	}
	if val, ok := resp2.Metadata["key2"]; !ok || val != 123 {
		t.Errorf("Expected metadata['key2'] = 123, got %v", val)
	}
}

func TestFromString(t *testing.T) {
	resp := FromString("Test message")
	if len(resp.Content) != 1 {
		t.Fatalf("Expected 1 content item, got %d", len(resp.Content))
	}
	if resp.Content[0].Text != "Test message" {
		t.Errorf("Expected content text 'Test message', got %s", resp.Content[0].Text)
	}
}

func TestFromError(t *testing.T) {
	testErr := errors.New("test error")
	resp, err := FromError(testErr)
	if resp != nil {
		t.Errorf("Expected nil response, got %v", resp)
	}
	if err != testErr {
		t.Errorf("Expected error to be passed through, got %v", err)
	}
}

func TestFormatResponse(t *testing.T) {
	testCases := []struct {
		name           string
		input          interface{}
		err            error
		expectError    bool
		expectedOutput string
	}{
		{
			name:           "nil response",
			input:          nil,
			err:            nil,
			expectError:    false,
			expectedOutput: `{"content":[]}`,
		},
		{
			name:           "error response",
			input:          nil,
			err:            errors.New("test error"),
			expectError:    true,
			expectedOutput: "",
		},
		{
			name:           "string response",
			input:          "Hello, world!",
			err:            nil,
			expectError:    false,
			expectedOutput: `{"content":[{"type":"text","text":"Hello, world!"}]}`,
		},
		{
			name:           "MCPResponse",
			input:          NewResponse().WithText("Test").WithMetadata("key", "value"),
			err:            nil,
			expectError:    false,
			expectedOutput: `{"content":[{"type":"text","text":"Test"}],"metadata":{"key":"value"}}`,
		},
		{
			name: "existing map with content",
			input: map[string]interface{}{
				"content": []interface{}{
					map[string]interface{}{
						"type": "text",
						"text": "Existing content",
					},
				},
			},
			err:            nil,
			expectError:    false,
			expectedOutput: `{"content":[{"text":"Existing content","type":"text"}]}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := FormatResponse(tc.input, tc.err)

			if tc.expectError && err == nil {
				t.Fatal("Expected error, got nil")
			}

			if !tc.expectError {
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}

				jsonData, err := json.Marshal(resp)
				if err != nil {
					t.Fatalf("Failed to marshal response: %v", err)
				}

				if string(jsonData) != tc.expectedOutput {
					t.Errorf("Expected output %s, got %s", tc.expectedOutput, string(jsonData))
				}
			}
		})
	}
}

func BenchmarkFormatResponse(b *testing.B) {
	testCases := []struct {
		name  string
		input interface{}
	}{
		{"string", "Hello, world!"},
		{"map", map[string]interface{}{"test": "value"}},
		{"MCPResponse", NewResponse().WithText("Test").WithMetadata("key", "value")},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = FormatResponse(tc.input, nil)
			}
		})
	}
}

func ExampleNewResponse() {
	// Create a new response with text content
	resp := NewResponse().WithText("Hello, world!")

	// Add metadata
	resp.WithMetadata("source", "example")

	// Convert to map for JSON serialization
	output, _ := json.Marshal(resp)
	fmt.Println(string(output))
	// Output: {"content":[{"type":"text","text":"Hello, world!"}],"metadata":{"source":"example"}}
}
