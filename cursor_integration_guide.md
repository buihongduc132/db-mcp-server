# Cursor Integration Guide for MCP Server

This guide explains how to integrate the MCP server with Cursor AI editor.

## Option 1: Using SSE Transport (Recommended)

SSE (Server-Sent Events) transport is the recommended method for integrating with Cursor as it allows more flexibility - you can run the server on a different machine, keep it running between Cursor sessions, and connect multiple Cursor instances to the same server.

### Setup Steps:

1. **Start the MCP server with SSE transport**:

   ```bash
   # Using the convenience script
   ./run_sse.sh
   
   # Or via Make
   make run-sse
   
   # Or manually
   ./mcp-server --transport sse --port 9090
   ```

2. **In Cursor Editor, configure the MCP server**:

   - Open Settings → AI → Advanced
   - Enable "Use a custom MCP server"
   - Enter URL: `http://localhost:9090/mcp`
   - Click "Save" and restart Cursor

3. **Verify Connection**:

   - In the MCP server terminal, you should see logs indicating a connection from Cursor
   - In Cursor, try using the AI assistant to verify it's working with your custom MCP server

### Troubleshooting SSE Connection:

- **Problem**: "Failed to create client" error
  **Solution**: The server now auto-generates a client ID when none is provided, so this should be resolved.

- **Problem**: Connection refused
  **Solution**: Ensure the server is running and the port matches what you specified in Cursor settings.

- **Problem**: 404 Not Found
  **Solution**: Make sure you're using the correct URL format: `http://localhost:PORT/mcp`

## Option 2: Using stdio Transport (Direct Integration)

The stdio transport allows Cursor to directly spawn and communicate with the MCP server. This is useful for local development and tight integration.

### Setup Steps:

1. **Create an integration script**:

   Create a file named `cursor_integration.sh` with the following content:

   ```bash
   #!/bin/bash
   
   # Change to the directory containing this script
   cd "$(dirname "$0")"
   
   # Run the MCP server with stdio transport
   ./mcp-server --transport stdio
   ```

2. **Make the script executable**:

   ```bash
   chmod +x cursor_integration.sh
   ```

3. **In Cursor Editor, configure the MCP server**:

   - Open Settings → AI → Advanced
   - Enable "Use a custom MCP server"
   - Enter Command: `/full/path/to/your/cursor_integration.sh`
   - Leave the URL field empty
   - Click "Save" and restart Cursor

### Troubleshooting stdio Connection:

- **Problem**: "Command not found" or similar errors
  **Solution**: Ensure the path to the script is correct and the script has execute permissions.

- **Problem**: Server starts but Cursor doesn't connect
  **Solution**: Check that the server is correctly using the stdio transport and that it's properly handling the JSON-RPC messages.

## Testing the Connection

Once connected, you can test if Cursor is properly communicating with your MCP server by:

1. Looking at the server logs for incoming requests
2. Using the AI assistant in Cursor to perform some tasks
3. Checking that the server responds to initialization and ping requests

## Advanced Configuration

### Custom Port:

If port 9090 is already in use, you can specify a different port:

```bash
./mcp-server --transport sse --port 8080
```

Then update the URL in Cursor settings accordingly.

### Running on a Different Machine:

To run the MCP server on a different machine and connect to it from Cursor:

1. Start the server on the remote machine with SSE transport
2. Ensure the port is open in any firewalls
3. In Cursor settings, use the remote machine's IP or hostname:
   ```
   http://remote-machine-ip:9090/mcp
   ```

## Next Steps

Once you have successfully integrated Cursor with your MCP server, you can:

1. Customize the server to add your own tools and resources
2. Extend the functionality to interact with other systems
3. Add authentication if needed for more secure deployments 