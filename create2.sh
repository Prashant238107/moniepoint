#!/bin/bash

# This script generates a JSON payload with a complex series of keys
# and sends it to the batch PUT endpoint of the key-value store.

echo "Generating JSON payload with multiple key series..."

# A generic value for all keys
VALUE="some_batch_value"

# Start the JSON array
JSON_PAYLOAD="["
is_first_element=true

# Helper function to append a key-value pair to the JSON payload
append_kv() {
  local key="$1"
  local value="$2"

  if [ "$is_first_element" = true ]; then
    is_first_element=false
  else
    JSON_PAYLOAD="$JSON_PAYLOAD,"
  fi
  JSON_PAYLOAD="$JSON_PAYLOAD{\"key\":\"$key\",\"value\":\"$value\"}"
}

# Series 1: key1 to key100, all keys serially
echo "Generating keys 1 to 100..."
for i in {1..100}; do
  append_kv "key$i" "$VALUE-series1"
done

# Series 2: key102 to key200, all even keys
echo "Generating even keys from 102 to 200..."
for i in $(seq 102 2 200); do
  append_kv "key$i" "$VALUE-series2"
done

# Series 3: key205 to key300, all keys in multiples of 5
echo "Generating keys in multiples of 5 from 205 to 300..."
for i in $(seq 205 5 300); do
  append_kv "key$i" "$VALUE-series3"
done

# Close the JSON array
JSON_PAYLOAD="$JSON_PAYLOAD]"

echo "Sending batch request to http://localhost:8080/moniepoint/kv/batch..."

# Send the request using curl.
# The payload is passed via stdin to avoid command line length limits.
echo "$JSON_PAYLOAD" 

echo ""
echo "Batch request sent successfully."
