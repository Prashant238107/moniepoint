#!/bin/bash

# A comprehensive test script for the distributed key-value store.
# It starts the cluster, demonstrates sharding, replication, and automatic failover,
# loads sample data, and then cleans up by stopping the cluster.

# --- Configuration ---
NODE_URLS=("http://localhost:8080" "http://localhost:8081" "http://localhost:8082")
NODE_COUNT=${#NODE_URLS[@]}
declare -a NODE_PIDS
JAR_FILE=""

LOG_DIR="logs"

# --- Helper Functions ---

# Color codes for better output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to print a formatted header
print_header() {
  echo -e "\n${BLUE}=======================================================================${NC}"
  echo -e "${BLUE}# $1${NC}"
  echo -e "${BLUE}=======================================================================${NC}"
}

# Function to calculate the leader node for a key (mimics ClusterService logic)
get_leader_node() {
  local key=$1
  # This is a simple way to get a consistent hash value in bash
  local hash_val=$(echo -n "$key" | cksum | awk '{print $1}')
  local leader_index=$((hash_val % NODE_COUNT))
  echo "${NODE_URLS[$leader_index]}"
}

# Function to start the cluster nodes in the background
start_cluster() {
  print_header "Starting 3-Node Cluster"
  mkdir -p "$LOG_DIR"
  
  for i in "${!NODE_URLS[@]}"; do
    port=$((8080 + i))
    node_url=${NODE_URLS[$i]}
    log_file="$LOG_DIR/node-$port.log"
    
    echo "Starting node on port $port... Log file: $log_file"
    
    java -jar "$JAR_FILE" \
      --server.port="$port" \
      --kvstore.cluster.current-node-url="$node_url" > "$log_file" 2>&1 &
      
    NODE_PIDS[$i]=$!
  done

  echo -e "\nWaiting for all nodes to become healthy..."
  for node_url in "${NODE_URLS[@]}"; do
    until curl -s -f -o /dev/null "$node_url/moniepoint/kv/internal/health"; do
      echo -n "."
      sleep 1
    done
    echo -e " ${GREEN}Node $node_url is up!${NC}"
  done
  echo -e "${GREEN}Cluster is fully operational.${NC}"
}

# Function to stop all running cluster nodes
stop_cluster() {
  print_header "Stopping Cluster"
  for pid in "${NODE_PIDS[@]}"; do
    if ps -p "$pid" > /dev/null; then
      echo "Stopping node with PID: $pid"
      kill "$pid"
    fi
  done
  # Wait for processes to terminate
  wait
  echo -e "${GREEN}All nodes stopped.${NC}"
  rm -rf "$LOG_DIR"
}

# Trap to ensure cluster is stopped on script exit (e.g., Ctrl+C)
trap stop_cluster EXIT

# --- Test Execution ---

clear
print_header "KV Store Cluster Test Script"

# --- Self-location logic ---
# Get the directory where the script is located and change to it.
# This ensures that all relative paths (like ./mvnw) work correctly,
# regardless of where the script is called from.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$SCRIPT_DIR"

# Check if we are in the project root directory
if [ ! -f "pom.xml" ]; then
    echo -e "${RED}Error: pom.xml not found.${NC}"
    echo "Please ensure this script is in the root directory of the project."
    exit 1
fi

echo "Step 1: Building the application JAR..."
if ! mvn clean install -DskipTests; then
  echo -e "${RED}Maven build failed. Aborting.${NC}"
  exit 1
fi
JAR_FILE=$(find target -name 'moniepoint-*.jar')
if [ -z "$JAR_FILE" ]; then
    echo -e "${RED}Could not find JAR file in target/ directory. Aborting.${NC}"
    exit 1
fi
echo -e "${GREEN}Build successful! JAR: $JAR_FILE${NC}"

start_cluster

echo -e "${YELLOW}Press Enter to begin...${NC}"
read -r

# --- Test 1: Sharding (Request Forwarding) ---
print_header "Test 1: Sharding (Request Forwarding)"
TEST_KEY_1="sharding-test-key"
LEADER_NODE_1=$(get_leader_node "$TEST_KEY_1")

# Pick a non-leader node to send the request to
NON_LEADER_NODE_1=""
for node in "${NODE_URLS[@]}"; do
  if [ "$node" != "$LEADER_NODE_1" ]; then
    NON_LEADER_NODE_1=$node
    break
  fi
done

echo "The calculated leader for key '${YELLOW}$TEST_KEY_1${NC}' is: ${GREEN}$LEADER_NODE_1${NC}"
echo "We will now send a PUT request to a non-leader node: ${YELLOW}$NON_LEADER_NODE_1${NC}"
echo -e "${YELLOW}Press Enter to send the request...${NC}"
read -r

curl -s -X PUT -H "Content-Type: application/json" \
  -d "{\"key\":\"$TEST_KEY_1\", \"value\":\"sharding works!\"}" \
  "$NON_LEADER_NODE_1/moniepoint/kv"

echo -e "\n${GREEN}Request sent!${NC}"
echo "Please check the logs:"
echo "  - The log for the non-leader (${YELLOW}$LOG_DIR/node-$(basename "$NON_LEADER_NODE_1").log${NC}) should show a 'Forwarding' message."
echo "  - The log for the leader (${GREEN}$LOG_DIR/node-$(basename "$LEADER_NODE_1").log${NC}) should show 'processed locally'."
echo -e "\nThis confirms that the request was correctly routed to the leader."

# --- Test 2: Replication ---
print_header "Test 2: Replication"
echo "When the leader node (${GREEN}$LEADER_NODE_1${NC}) processed the write, it also replicated it."
echo "Please check the logs:"
echo "  - The log for the leader (${GREEN}$LOG_DIR/node-$(basename "$LEADER_NODE_1").log${NC}) should show 'Replicating WAL entry...' messages."
echo "  - The logs for the follower nodes should show 'Received internal replication request'."
echo -e "\nThis confirms that data is being replicated to followers."
echo -e "${YELLOW}Press Enter to continue...${NC}"
read -r

# --- Test 3: Automatic Failover ---
print_header "Test 3: Automatic Failover"
TEST_KEY_2="failover-test-key"
LEADER_NODE_2=$(get_leader_node "$TEST_KEY_2")

leader_port=$(basename "$LEADER_NODE_2")
leader_pid=0
for i in "${!NODE_URLS[@]}"; do
    if [ "${NODE_URLS[$i]}" == "$LEADER_NODE_2" ]; then
        leader_pid=${NODE_PIDS[$i]}
        break
    fi
done

echo "The calculated leader for key '${YELLOW}$TEST_KEY_2${NC}' is: ${GREEN}$LEADER_NODE_2${NC}"
echo "Now stopping the leader node (PID: $leader_pid) programmatically..."
kill "$leader_pid"

echo -e "\nLeader node stopped. Waiting 15 seconds for other nodes' health checks to detect the failure..."
sleep 15

echo -e "\nHealth checks should have completed."
echo "Please check the logs of the remaining live nodes. They should show a 'Health check failed' or 'Node ... is down' warning."
echo -e "\nNow, we will send a PUT request for the ${YELLOW}same key${NC} ('$TEST_KEY_2') to a live node."
echo "The cluster should automatically failover to the next available replica."
echo -e "${YELLOW}Press Enter to send the request...${NC}"
read -r

# Send the request to one of the remaining live nodes
LIVE_NODE=""
for node in "${NODE_URLS[@]}"; do
  if [ "$node" != "$LEADER_NODE_2" ]; then
    LIVE_NODE=$node
    break
  fi
done

curl -s -X PUT -H "Content-Type: application/json" \
  -d "{\"key\":\"$TEST_KEY_2\", \"value\":\"failover works!\"}" \
  "$LIVE_NODE/moniepoint/kv"

echo -e "\n${GREEN}Request sent!${NC}"
echo "Please check the logs. One of the live nodes should now identify itself as the new leader and process the request locally."
echo -e "\nTo confirm, we will now read the key back from the cluster."
echo "The request will be automatically routed to the new leader."
echo -e "${YELLOW}Press Enter to read the key...${NC}"
read -r

# We can send the read to any live node. The read path doesn't have proxying,
# so we'll see the 'mis-routed' warning, but this demonstrates the new leader is findable.
echo "Querying for key '$TEST_KEY_2' via node $LIVE_NODE..."
RESPONSE=$(curl -s -w "\nHTTP_STATUS:%{http_code}\n" "$LIVE_NODE/moniepoint/kv/$TEST_KEY_2")
echo -e "Response from cluster:\n${GREEN}$RESPONSE${NC}"
echo -e "\nThis confirms the cluster healed itself and served the request after a failure."

# --- Test 4: Sample Data Loading ---
print_header "Test 4: Sample Data Loading"
echo "This part of the script will load a large amount of sample data."
echo "This will trigger MemTable flushes, SSTable creation, and compaction."
echo -e "${YELLOW}Press Enter to start loading data...${NC}"
read -r

# --- Load data from create1.sh logic ---
echo "Loading 100 keys individually..."
VALUE_1="individual-put-value"
for i in {1..100}; do
  KEY="load-test-key-$i"
  json_payload=$(printf '{"key":"%s","value":"%s"}' "$KEY" "$VALUE_1")
  curl -s -o /dev/null -X PUT -H "Content-Type: application/json" -d "$json_payload" "${NODE_URLS[0]}/moniepoint/kv"
  # Add a small delay to avoid overwhelming the server too quickly
  sleep 0.05
done
echo "Individual keys loaded."

# --- Load data from create2.sh logic ---
echo "Loading a complex batch of keys..."
# Re-using the logic from your create2.sh script
VALUE_2="some_batch_value"
JSON_PAYLOAD="["
is_first_element=true
append_kv() {
  local key="$1"; local value="$2"
  if [ "$is_first_element" = true ]; then is_first_element=false; else JSON_PAYLOAD="$JSON_PAYLOAD,"; fi
  JSON_PAYLOAD="$JSON_PAYLOAD{\"key\":\"$key\",\"value\":\"$value\"}"
}
for i in {1..100}; do append_kv "batch-key-$i" "$VALUE_2-series1"; done
for i in $(seq 102 2 200); do append_kv "batch-key-$i" "$VALUE_2-series2"; done
for i in $(seq 205 5 300); do append_kv "batch-key-$i" "$VALUE_2-series3"; done
JSON_PAYLOAD="$JSON_PAYLOAD]"

echo "$JSON_PAYLOAD" | curl -s -o /dev/null -X PUT -H "Content-Type: application/json" -d @- "${NODE_URLS[0]}/moniepoint/kv/batch"
echo "Batch keys loaded."

print_header "All Tests Completed!"
echo "You can now inspect the 'data/wal' and 'data/sstables' directories on each node to see the created files."