#!/usr/bin/env python3
"""
Simple STDIO test script for db-mcp-server
"""

import json
import subprocess
import os
import time

def main():
    # Path to the mcp-server binary
    server_path = "../mcp-server"
    
    # Prepare environment
    env = os.environ.copy()
    env["SKIP_DB"] = "true"
    
    # Start the server process with STDIO transport
    print("Starting server process...")
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
        
        # Send request
        request_str = json.dumps(request) + "\n"
        server.stdin.write(request_str)
        server.stdin.flush()
        
        # Read response (wait for up to 10 seconds)
        end_time = time.time() + 10
        response_found = False
        
        print("Waiting for response...")
        while time.time() < end_time and not response_found:
            line = server.stdout.readline().strip()
            if line.startswith("MCPRPC:"):
                json_str = line[7:]  # Remove MCPRPC: prefix
                print("\nReceived response:")
                pretty_json = json.dumps(json.loads(json_str), indent=2)
                print(pretty_json)
                response_found = True
        
        if not response_found:
            print("No response received within timeout")
        
    except Exception as e:
        print(f"Error during test: {e}")
    finally:
        print("\nStopping server...")
        server.terminate()
        
        # Show stderr
        stderr = server.stderr.read()
        if stderr:
            print("\nServer stderr output:")
            print(stderr)

if __name__ == "__main__":
    main() 