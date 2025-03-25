# STDIO Transport Examples

This directory contains examples for using the STDIO transport with the db-mcp-server.

## Testing the STDIO Transport

To test the STDIO transport, use the provided scripts:

1. `simple_stdio_test.py` - A simple test that sends an initialize request and displays the response
2. `test_stdio.py` - A more comprehensive test that tests multiple requests

### Running the Tests

Make sure the scripts are executable:

```bash
chmod +x *.py
```

Then run either test:

```bash
# Run the simple test
./simple_stdio_test.py

# Run the comprehensive test
./test_stdio.py
```

### Writing Your Own Client

The STDIO transport communicates using JSON-RPC 2.0 over standard input/output. To write your own client:

1. Start the server with the `-t stdio` flag: `./mcp-server -t stdio`
2. Send JSON-RPC requests as JSON objects, one per line, to stdin
3. Read JSON-RPC responses from stdout, prefixed with `MCPRPC:`

#### Example Request

```json
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{"tools":true},"clientInfo":{"name":"My Client","version":"1.0.0"}}}
```

#### Example Response

```
MCPRPC:{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05","serverInfo":{"name":"MCP Server","version":"1.0.0"},"capabilities":{"tools":{"available":[...]}}}}
```

## Integration with Other Tools

The STDIO transport is useful for integrating the MCP server with other tools or languages without requiring HTTP/SSE support. Some examples:

- Shell scripts using pipes
- Language bindings in Python, Go, etc.
- Text editors or IDEs
- Command-line tools

## Using the SKIP_DB Environment Variable

For testing purposes, you can set the `SKIP_DB=true` environment variable to skip database initialization:

```bash
SKIP_DB=true ./mcp-server -t stdio
```

This is useful for testing the STDIO transport without requiring a database connection. 