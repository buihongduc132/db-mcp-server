#!/usr/bin/env python3
"""
Test script for db-mcp-server STDIO transport
This script sends JSON-RPC requests to the server via stdin and reads responses from stdout.
"""

import json
import subprocess
import sys
import time
import os

def main():
    # Path to the mcp-server binary
    server_path = "../mcp-server"
    
    # Start the server process with STDIO transport
    print("Starting server process...")
    env = os.environ.copy()
    env["SKIP_DB"] = "true"
    
    server = subprocess.Popen(
        [server_path, "-t", "stdio"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env
    )
    
    # Wait for server to initialize
    time.sleep(1)
    
    try:
        # Send initialize request
        print("Sending initialize request...")
        request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": True
                },
                "clientInfo": {
                    "name": "STDIO Test Client",
                    "version": "1.0.0"
                }
            }
        }
        
        send_request(server, request)
        response = read_response(server)
        print_response(response)
        
        # Send tools/list request
        print("\nSending tools/list request...")
        request = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {}
        }
        
        send_request(server, request)
        response = read_response(server)
        print_response(response)
        
        # If there are tools available, attempt to call one
        if response and "result" in response and "tools" in response["result"]:
            tools = response["result"]["tools"]
            if tools:
                tool_name = tools[0]["name"]
                print(f"\nFound tool: {tool_name}, sending tools/call request...")
                
                # Set up arguments based on tool input schema
                tool_args = {}
                if "inputSchema" in tools[0] and "properties" in tools[0]["inputSchema"]:
                    for prop, details in tools[0]["inputSchema"]["properties"].items():
                        # Provide a default value for each property
                        if "type" in details:
                            if details["type"] == "string":
                                tool_args[prop] = "test"
                            elif details["type"] == "integer" or details["type"] == "number":
                                tool_args[prop] = 1
                            elif details["type"] == "boolean":
                                tool_args[prop] = True
                
                request = {
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "tools/call",
                    "params": {
                        "name": tool_name,
                        "arguments": tool_args
                    }
                }
                
                send_request(server, request)
                response = read_response(server)
                print_response(response)
        
    except Exception as e:
        print(f"Error during test: {e}")
    finally:
        # Send SIGINT to the server
        print("\nStopping server...")
        server.terminate()
        
        # Collect any remaining output
        stdout, stderr = server.communicate(timeout=5)
        if stdout:
            print("Remaining stdout:", stdout)
        if stderr:
            print("Stderr:", stderr)

def send_request(server, request):
    """Send a JSON-RPC request to the server."""
    request_str = json.dumps(request) + "\n"
    server.stdin.write(request_str)
    server.stdin.flush()

def read_response(server):
    """Read a JSON-RPC response from the server."""
    # Continue reading lines until we find a valid JSON response or timeout
    start_time = time.time()
    timeout = 5  # Timeout in seconds
    
    while time.time() - start_time < timeout:
        line = server.stdout.readline().strip()
        if not line:
            time.sleep(0.1)
            continue
            
        # Look for our specific MCPRPC prefix
        if line.startswith("MCPRPC:"):
            # Extract the JSON part
            json_str = line[7:]  # Skip "MCPRPC:"
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                print(f"Failed to parse JSON from MCPRPC message: {json_str}")
                continue
        else:
            # Skip all other output
            continue
    
    # If we get here, we timed out without finding a valid JSON response
    print("Timeout waiting for response")
    return None

def print_response(response):
    """Print a formatted JSON-RPC response."""
    if not response:
        print("No response received")
        return
    
    print(json.dumps(response, indent=2))

if __name__ == "__main__":
    main() 