#!/bin/bash

echo "Sending individual PUT requests..."

# The value for each key. It's intentionally long to fill the memtable faster.
VALUE="a_very_long_string_to_ensure_we_cross_the_memtable_size_threshold_multiple_times_during_this_batch_operation"

# Send multiple individual requests
for i in {301..100000}
do
  KEY="key$i"
  echo "Sending request for key: $KEY"

  # Safely create the JSON payload
  json_payload=$(printf '{"key":"%s","value":"%s"}' "$KEY" "$VALUE")

  # Send a single PUT request using curl to the correct endpoint
  curl -s -o /dev/null -X PUT -H "Content-Type: application/json" -d "$json_payload" "http://localhost:8080/moniepoint/kv"
done

echo ""
echo "All requests sent successfully."
