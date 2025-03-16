# Integrating with Cursor Editor

The MCP server now supports integration with Cursor Editor through its Model Context Protocol (MCP). This allows the Cursor AI assistant to use your database as a tool for data operations.

## Cursor MCP Endpoint

The MCP server exposes a dedicated endpoint for Cursor at:

```
http://localhost:9090/cursor-mcp
```

This endpoint follows the Cursor MCP specification for Server-Sent Events (SSE) transport.

## Available Tools

The following database tools are available to Cursor:

1. **execute_query**: Execute a SQL query and return the results
2. **insert_data**: Insert data into a table
3. **update_data**: Update data in a table
4. **delete_data**: Delete data from a table

## Setting Up Cursor Integration

### Local Setup

1. Start your MCP server:

```bash
make run-mysql
```

2. In Cursor, open the Command Palette (Cmd+Shift+P or Ctrl+Shift+P) and select "Configure External Tools"

3. Add a new MCP connection with the following configuration:

```json
{
  "name": "Database Access",
  "description": "Provides tools to interact with a database",
  "transport": {
    "type": "sse",
    "serverUrl": "http://localhost:9090/cursor-mcp"
  }
}
```

4. Save the configuration and restart Cursor

### Remote Setup

If your MCP server is running on a remote machine:

1. Ensure your server is accessible from the Cursor client (consider security implications)

2. Use the remote URL in the configuration:

```json
{
  "name": "Remote Database Access",
  "description": "Provides tools to interact with a remote database",
  "transport": {
    "type": "sse",
    "serverUrl": "http://your-server-address:9090/cursor-mcp"
  }
}
```

## Using the Tools in Cursor

Once configured, you can use natural language to ask Cursor to interact with your database. For example:

- "Show me all users in the database"
- "Insert a new product with name 'Laptop' and price 999.99"
- "Update the email of user with ID 5"
- "Delete all orders older than January 1st"

Cursor will translate these requests into appropriate tool calls to your MCP server.

## Security Considerations

- **Authentication**: The current implementation does not include authentication. Consider adding authentication for production use.
- **SQL Injection**: While the tools execute raw SQL, in a production environment you should add validation to prevent SQL injection.
- **Access Control**: Consider limiting what tables and operations are accessible through the tools.

## Troubleshooting

If you encounter issues:

1. Check the MCP server logs for errors
2. Ensure the Cursor MCP endpoint is accessible from your Cursor client
3. Verify your database connection is working properly 