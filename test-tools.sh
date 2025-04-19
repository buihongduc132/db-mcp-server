#!/bin/bash

# Test script for MCP database tools
# This script tests all the tools implemented in the MCP server

# Set up colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Function to display help message
show_help() {
    echo -e "${CYAN}MCP Database Tools Test Script${NC}"
    echo ""
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --parallel         Run tests in parallel"
    echo "  --timeout SECONDS  Set timeout for each test (default: 5 seconds)"
    echo "  --help             Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                           # Run tests sequentially with default timeout"
    echo "  $0 --parallel                # Run tests in parallel with default timeout"
    echo "  $0 --timeout 10              # Run tests sequentially with 10s timeout"
    echo "  $0 --parallel --timeout 10   # Run tests in parallel with 10s timeout"
    echo ""
    exit 0
}

# Create output directory for test results
mkdir -p inspector

# Default timeout in seconds
TIMEOUT=5

# Default parallel execution flag
PARALLEL=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --parallel)
      PARALLEL=true
      shift
      ;;
    --timeout)
      TIMEOUT="$2"
      shift 2
      ;;
    --help)
      show_help
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

echo -e "${BLUE}Running tests with timeout: ${TIMEOUT}s, parallel: ${PARALLEL}${NC}"

# Function to run a test and save the output
run_test() {
    local test_name=$1
    local method=$2
    local params=$3
    local output_file=$4

    echo -e "${YELLOW}Running test: ${test_name}${NC}"
    echo "Method: $method"
    echo "Params: $params"

    # Create a temporary file for the request
    local request_file=$(mktemp)
    local temp_output=$(mktemp)
    local temp_error=$(mktemp)

    # Generate a unique ID for the request
    local id=$(date +%s%N | md5sum | head -c 8)

    # Create the MCP request JSON
    echo '{"jsonrpc":"2.0","id":"'$id'","method":"'$method'","params":'$params'}' > "$request_file"

    echo -e "${BLUE}Request:${NC}"
    cat "$request_file"
    echo ""

    # Run the command with timeout
    echo -e "${BLUE}Executing request...${NC}"
    timeout $TIMEOUT cat "$request_file" | ./inspector/stdio-wrapper.sh > "$temp_output" 2> "$temp_error"
    local exit_code=$?

    # Check if the command timed out
    if [ $exit_code -eq 124 ]; then
        echo -e "${YELLOW}Command timed out after ${TIMEOUT}s${NC}"
        echo '{"error": "Test timed out after '$TIMEOUT' seconds"}' > "$output_file"
        echo ""
        rm -f "$request_file" "$temp_output" "$temp_error"
        return 1
    fi

    # Check if the output file has content
    if [ -s "$temp_output" ]; then
        # Copy the output to the final destination
        cp "$temp_output" "$output_file"

        echo -e "${BLUE}Response:${NC}"
        head -20 "$output_file"
        if [ $(wc -l < "$output_file") -gt 20 ]; then
            echo -e "${BLUE}... (output truncated, see $output_file for full response)${NC}"
        fi
        echo ""

        # Check if the output contains valid JSON
        if grep -q '{' "$output_file" && grep -q '}' "$output_file"; then
            # Check if the response contains an error
            if grep -q '"error"' "$output_file"; then
                echo -e "${RED}Test failed: ${test_name} (MCP error response)${NC}"
                echo -e "Error: $(grep -o '"error":[^}]*' "$output_file" | head -1)"
                echo ""
                rm -f "$request_file" "$temp_output" "$temp_error"
                return 1
            else
                echo -e "${GREEN}Test completed successfully: ${test_name}${NC}"
                echo -e "Output saved to: $output_file"
                echo ""
                rm -f "$request_file" "$temp_output" "$temp_error"
                return 0
            fi
        else
            echo -e "${RED}Test failed: ${test_name} (invalid JSON output)${NC}"
            echo -e "Output saved to: $output_file"
            echo ""
            rm -f "$request_file" "$temp_output" "$temp_error"
            return 1
        fi
    else
        # Check if there's any error output
        if [ -s "$temp_error" ]; then
            echo -e "${RED}Test failed: ${test_name} (error occurred)${NC}"
            echo -e "Error output:"
            cat "$temp_error"
        else
            echo -e "${RED}Test failed: ${test_name} (no output)${NC}"
        fi

        # Create an empty output file to indicate failure
        echo '{"error": "Test failed with no output"}' > "$output_file"
        echo ""
        rm -f "$request_file" "$temp_output" "$temp_error"
        return 1
    fi
}

# For stdio transport, we don't need to start a background server
# Instead, we'll create a wrapper script that will be used for each test

echo "Creating stdio wrapper script..."
cat > inspector/stdio-wrapper.sh << 'EOF'
#!/bin/bash

# This script wraps the stdio server for testing
# It takes JSON input from stdin, passes it to the server, and returns the output

# Create log directory if it doesn't exist
mkdir -p inspector/logs

# Generate a unique ID for this run
RUN_ID=$(date +"%Y%m%d_%H%M%S_%N")
LOG_FILE="inspector/logs/stdio_${RUN_ID}.log"

# Log the start of the script
echo "[$(date +"%Y-%m-%d %H:%M:%S")] Starting stdio wrapper script (Run ID: ${RUN_ID})" | tee -a "$LOG_FILE"

# Create a temporary file to store the input
INPUT_FILE=$(mktemp)

# Read input from stdin and save it to the temporary file
cat > "$INPUT_FILE"

# Log the input
echo "[$(date +"%Y-%m-%d %H:%M:%S")] Input:" | tee -a "$LOG_FILE"
cat "$INPUT_FILE" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# Create a named pipe for communication
INPUT_PIPE=$(mktemp -u)
mkfifo $INPUT_PIPE

# Log the server start
echo "[$(date +"%Y-%m-%d %H:%M:%S")] Starting server with stdio transport" | tee -a "$LOG_FILE"

# Start the server with stdio transport and capture output
./bin/server -t stdio -c config.json < $INPUT_PIPE > >(tee -a "$LOG_FILE") 2> >(tee -a "$LOG_FILE" >&2) &
SERVER_PID=$!

# Log the server PID
echo "[$(date +"%Y-%m-%d %H:%M:%S")] Server started with PID: $SERVER_PID" | tee -a "$LOG_FILE"

# Write the input to the pipe
echo "[$(date +"%Y-%m-%d %H:%M:%S")] Sending input to server" | tee -a "$LOG_FILE"
cat "$INPUT_FILE" > $INPUT_PIPE

# Wait for the server to process the input and exit
echo "[$(date +"%Y-%m-%d %H:%M:%S")] Waiting for server to complete" | tee -a "$LOG_FILE"
wait $SERVER_PID
EXIT_CODE=$?

# Log the server exit
echo "[$(date +"%Y-%m-%d %H:%M:%S")] Server exited with code: $EXIT_CODE" | tee -a "$LOG_FILE"

# Clean up
echo "[$(date +"%Y-%m-%d %H:%M:%S")] Cleaning up temporary files" | tee -a "$LOG_FILE"
rm -f $INPUT_PIPE
rm -f "$INPUT_FILE"

echo "[$(date +"%Y-%m-%d %H:%M:%S")] Stdio wrapper script completed. Log saved to: $LOG_FILE" | tee -a "$LOG_FILE"
EOF

chmod +x inspector/stdio-wrapper.sh

echo -e "${GREEN}Stdio wrapper script created with enhanced logging.${NC}"
echo -e "${BLUE}Logs will be saved to inspector/logs/stdio_*.log${NC}"

# Define the database to use
DATABASE="bide_pg"

# Create a function to generate MCP request JSON
generate_mcp_request() {
    local method=$1
    local params=$2

    # Generate a unique ID for the request
    local id=$(date +%s%N | md5sum | head -c 8)

    # Create the MCP request JSON
    echo '{"jsonrpc":"2.0","id":"'$id'","method":"'$method'","params":'$params'}'
}

# Define all tests with their parameters
declare -A test_methods
declare -A test_params

# Define test methods and parameters
test_methods["List Databases"]="tools/list"
test_params["List Databases"]="{}"

# For all other tools, we need to call them directly
test_methods["SQL Tool"]="sql"
test_params["SQL Tool"]="{\"sql\":\"SELECT current_database()\", \"database\":\"${DATABASE}\"}"

test_methods["Database Statistics"]="db_stats"
test_params["Database Statistics"]="{\"database\":\"${DATABASE}\"}"

test_methods["Table Statistics"]="table_stats"
test_params["Table Statistics"]="{\"database\":\"${DATABASE}\", \"table\":\"users\"}"

test_methods["Get Indexes"]="get_indexes"
test_params["Get Indexes"]="{\"database\":\"${DATABASE}\"}"

test_methods["Get Constraints"]="get_constraints"
test_params["Get Constraints"]="{\"database\":\"${DATABASE}\"}"

test_methods["Get Views"]="get_views"
test_params["Get Views"]="{\"database\":\"${DATABASE}\"}"

test_methods["Get Types"]="get_types"
test_params["Get Types"]="{\"database\":\"${DATABASE}\"}"

test_methods["Get Schemas"]="get_schemas"
test_params["Get Schemas"]="{\"database\":\"${DATABASE}\"}"

test_methods["Get Sample Data"]="get_sample_data"
test_params["Get Sample Data"]="{\"database\":\"${DATABASE}\", \"table\":\"users\", \"limit\":5}"

test_methods["Get Unique Values"]="get_unique_values"
test_params["Get Unique Values"]="{\"database\":\"${DATABASE}\", \"table\":\"users\", \"column\":\"email\", \"limit\":5}"

# Array to store background PIDs if running in parallel
declare -a pids

# Run tests either in parallel or sequentially
if [ "$PARALLEL" = true ]; then
    echo -e "${BLUE}Running tests in parallel...${NC}"

    # Run all tests in parallel
    for test_name in "${!test_methods[@]}"; do
        # Convert test name to filename
        output_file="inspector/$(echo "$test_name" | tr ' ' '_' | tr '[:upper:]' '[:lower:]').json"

        # Run test in background
        (run_test "$test_name" "${test_methods[$test_name]}" "${test_params[$test_name]}" "$output_file") &
        pids+=($!)
    done

    # Wait for all background processes to complete
    for pid in "${pids[@]}"; do
        wait $pid
    done
else
    echo -e "${BLUE}Running tests sequentially...${NC}"

    # Run tests one by one
    for test_name in "${!test_methods[@]}"; do
        # Convert test name to filename
        output_file="inspector/$(echo "$test_name" | tr ' ' '_' | tr '[:upper:]' '[:lower:]').json"

        # Run test
        run_test "$test_name" "${test_methods[$test_name]}" "${test_params[$test_name]}" "$output_file"
    done
fi

# No need to kill a server since we're using stdio transport
echo "All tests completed."

# Generate summary of test results
echo -e "\n${CYAN}Test Results Summary:${NC}"
echo -e "${CYAN}====================${NC}"

# Count successful and failed tests
SUCCESS_COUNT=0
FAIL_COUNT=0
TOTAL_COUNT=0

echo -e "${CYAN}Tool Name                  | Status | File${NC}"
echo -e "${CYAN}--------------------------|--------|------------------${NC}"

for test_name in "${!test_methods[@]}"; do
    # Convert test name to filename
    output_file="inspector/$(echo "$test_name" | tr ' ' '_' | tr '[:upper:]' '[:lower:]').json"
    TOTAL_COUNT=$((TOTAL_COUNT + 1))

    # Check if file exists and has content
    if [ -s "$output_file" ] && ! grep -q '"error"' "$output_file"; then
        STATUS="${GREEN}SUCCESS${NC}"
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    else
        STATUS="${RED}FAILED ${NC}"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi

    # Pad test name to fixed width
    printf "%-26s | %b | %s\n" "$test_name" "$STATUS" "$output_file"
done

echo -e "\n${CYAN}Summary:${NC}"
echo -e "  Total tests: $TOTAL_COUNT"
echo -e "  ${GREEN}Successful: $SUCCESS_COUNT${NC}"
echo -e "  ${RED}Failed: $FAIL_COUNT${NC}"

echo -e "\nAll tests completed. Results saved in the 'inspector' directory."
echo "You can examine the JSON files to see the output of each tool."
echo -e "Detailed logs for each test are available in the 'inspector/logs' directory."
echo -e "Log format: stdio_YYYYMMDD_HHMMSS_NNNNNNNNN.log"

# Count log files
LOG_COUNT=$(ls -1 inspector/logs/stdio_*.log 2>/dev/null | wc -l)
echo -e "${CYAN}Total log files: ${LOG_COUNT}${NC}"

# Show the most recent log files
if [ $LOG_COUNT -gt 0 ]; then
    echo -e "${CYAN}Most recent logs:${NC}"
    ls -lt inspector/logs/stdio_*.log | head -5 | awk '{print "  " $9 " (" $6 " " $7 " " $8 ")"}'
    echo -e "\nTo view a log file, use: cat <log_file_path>"
fi
