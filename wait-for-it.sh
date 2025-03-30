#!/bin/sh
# wait-for-it.sh

set -e

host="$1"
shift
cmd=""

# Process options
while [ $# -gt 0 ]; do
  case "$1" in
    -t)
      timeout="$2"
      shift 2
      ;;
    *)
      cmd="$cmd $1"
      shift
      ;;
  esac
done

# Default timeout
timeout=${timeout:-30}

echo "Waiting for $host to be available..."
start_time=$(date +%s)
end_time=$((start_time + timeout))

until nc -z -w1 ${host/:/ }; do
  current_time=$(date +%s)
  if [ $current_time -gt $end_time ]; then
    echo "Timeout waiting for $host to be available"
    exit 1
  fi
  echo "Waiting for $host to be available... ($(($end_time - $current_time))s timeout remaining)"
  sleep 1
done

echo "$host is available"

if [ ! -z "$cmd" ]; then
  echo "Executing command:$cmd"
  exec $cmd
fi 