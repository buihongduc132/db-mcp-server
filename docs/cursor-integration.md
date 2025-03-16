# Cursor Integration Guide

This guide explains how to integrate the MCP Server with Cursor Editor using the Model Context Protocol (MCP).

## What is MCP?

The Model Context Protocol (MCP) is an open protocol that standardizes how applications provide context and tools to LLMs. In Cursor, it allows the AI assistant to interact with external tools and data sources, such as our database server.

## Transport Modes

The MCP Server supports two transport modes for integration with Cursor:

1. **stdio**: For local development, where Cursor runs the server as a subprocess
2. **SSE**: For production, where the server runs independently and Cursor connects via HTTP

## Setting Up in Cursor

### Step 1: Create a Configuration File

Create a `.cursor/mcp.json` file in your project directory or in your home directory (`~/.cursor/mcp.json`) for global access.

#### For stdio Transport (Local Development)

```json
{
  "mcpServers": {
    "db-server": {
      "command": "/path/to/mcp-server",
      "args": ["-transport", "stdio"],
      "env": {
        "DB_TYPE": "mysql",
        "DB_HOST": "localhost",
        "DB_PORT": "3306",
        "DB_USER": "your_username",
        "DB_PASSWORD": "your_password",
        "DB_NAME": "your_database"
      }
    }
  }
}
```

Replace `/path/to/mcp-server` with the absolute path to your compiled server binary.

#### For SSE Transport (Production)

```json
{
  "mcpServers": {
    "db-server": {
      "url": "http://localhost:9090/sse"
    }
  }
}
```

Make sure the server is running before connecting Cursor.

### Step 2: Start the Server (SSE Mode Only)

If using SSE transport, start the server:

```bash
cd /path/to/mcp-server
make run-sse
```

For stdio transport, Cursor will automatically start the server as a subprocess.

### Step 3: Verify Connection

1. Open Cursor
2. Go to Settings > Features > MCP
3. You should see your server listed under "Available MCP Servers"
4. Click the refresh button if it doesn't appear
5. Your server's tools should be listed under "Available Tools"

## Using the Database Tools in Cursor

Once connected, you can use the database tools in Cursor's AI assistant. Here are some example prompts:

- "Show me the schema of the users table"
- "Query all users who signed up in the last month"
- "Insert a new record into the products table"
- "Update the email for user with ID 123"

## Troubleshooting

### Server Not Appearing in Cursor

- Check that the server is running (for SSE mode)
- Verify the path to the server binary (for stdio mode)
- Check the `.cursor/mcp.json` file for syntax errors
- Restart Cursor

### Connection Errors

- Check database credentials in the `.env` file or environment variables
- Verify the database server is running and accessible
- Check firewall settings
- Use the `/debug/connection` endpoint to diagnose issues:
  ```
  http://localhost:9090/debug/connection
  ```

### Permission Issues

If you encounter permission issues with the server binary:

```bash
cd /path/to/mcp-server
make fix-permissions
```

## Advanced Configuration

### Custom Port

To use a custom port:

```json
{
  "mcpServers": {
    "db-server": {
      "url": "http://localhost:8080/sse"
    }
  }
}
```

And start the server with:

```bash
make run-sse PORT=8080
```

### Multiple Database Connections

You can configure multiple MCP servers for different databases:

```json
{
  "mcpServers": {
    "production-db": {
      "url": "http://localhost:9090/sse"
    },
    "development-db": {
      "command": "/path/to/mcp-server",
      "args": ["-transport", "stdio", "-db-type", "mysql", "-db-config", "dev_user:password@tcp(localhost:3306)/dev_db"]
    }
  }
}
```

## Security Considerations

- Use environment variables for sensitive information
- Consider using a firewall to restrict access to the server
- For production environments, consider adding authentication
- Never expose the server to the public internet without proper security measures 