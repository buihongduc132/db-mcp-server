#!/usr/bin/env node

const { spawn } = require('child_process');
const path = require('path');

// Function to send a JSON-RPC request and get a response
async function sendRequest(serverProcess, request) {
  return new Promise((resolve, reject) => {
    // Set up data listener
    let responseData = '';
    const dataListener = (data) => {
      const strData = data.toString();
      console.log(`Received data: ${strData}`);
      responseData += strData;
      
      try {
        // Check if we have a complete JSON-RPC response
        const json = JSON.parse(responseData);
        if (json.id === request.id) {
          // We got a complete response
          serverProcess.stdout.removeListener('data', dataListener);
          resolve(json);
        }
      } catch (e) {
        // Incomplete JSON, keep listening
      }
    };
    
    // Set up error listener
    serverProcess.stderr.on('data', (data) => {
      console.error(`Server stderr: ${data}`);
    });
    
    // Add data listener
    serverProcess.stdout.on('data', dataListener);
    
    // Send the request
    const requestStr = JSON.stringify(request) + '\n';
    console.log(`Sending request: ${requestStr}`);
    serverProcess.stdin.write(requestStr);
  });
}

// Main function to run the test
async function main() {
  // Path to the server binary
  const serverBin = path.join(process.cwd(), 'server');
  
  // Check if the binary exists
  const fs = require('fs');
  if (!fs.existsSync(serverBin)) {
    console.error(`Server binary not found at ${serverBin}`);
    process.exit(1);
  }
  
  // Spawn the server process
  console.log(`Starting server: ${serverBin} -t stdio`);
  const serverProcess = spawn(serverBin, ['-t', 'stdio']);
  
  // Handle server exit
  serverProcess.on('exit', (code, signal) => {
    console.log(`Server exited with code ${code} and signal ${signal}`);
  });
  
  try {
    // Send a request to list available tools
    const listToolsRequest = {
      jsonrpc: "2.0",
      method: "tools/list",
      params: {},
      id: 1
    };
    
    console.log("Sending tools/list request...");
    const listToolsResponse = await sendRequest(serverProcess, listToolsRequest);
    console.log('List tools response:', JSON.stringify(listToolsResponse, null, 2));
    
    // If we have any database tools, try to use one
    if (listToolsResponse.result && listToolsResponse.result.tools) {
      const dbTools = listToolsResponse.result.tools.filter(tool => 
        tool.name.startsWith('list_databases'));
      
      if (dbTools.length > 0) {
        // Define the request to list databases
        const listDatabasesRequest = {
          jsonrpc: "2.0",
          method: "tools/call",
          params: {
            name: "list_databases",
            arguments: {}
          },
          id: 2
        };
        
        console.log("Sending list_databases tool call request...");
        const listDatabasesResponse = await sendRequest(serverProcess, listDatabasesRequest);
        console.log('List databases response:', JSON.stringify(listDatabasesResponse, null, 2));
      }
    }
    
    // Clean exit
    console.log('Test completed successfully');
    serverProcess.kill();
    process.exit(0);
  } catch (error) {
    console.error('Error during test:', error);
    serverProcess.kill();
    process.exit(1);
  }
}

// Run the test
main().catch(err => {
  console.error('Unhandled error:', err);
  process.exit(1);
}); 