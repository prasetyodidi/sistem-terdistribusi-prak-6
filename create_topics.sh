#!/bin/bash

# Define Kafka broker(s) - can be a single broker or comma-separated list
<comment-tag id="1">KAFKA_BROKERS="kafka1:9092,kafka2:9092,kafka3:9092"  # Default, can be overridden</comment-tag id="1">

# Define the kafka-topics.sh path
KAFKA_TOPICS_PATH="/usr/bin/kafka-topics" # Corrected path

# Function to create a Kafka topic
create_topic() {
  local topic_name="$1"
  local partitions="$2"
  local replication_factor="$3"
  local configs=("$@") #capture all the arguments

  # Remove the first 3 arguments (topic name, partitions, and replication factor)
  shift 3
  local topic_configs=()
  # Convert the rest of the arguments to the --config format.
  for arg in "$@"; do
    IFS='=' read -r key value <<< "$arg"
    topic_configs+=("--config" "$key=$value")
  done

  if ! [[ -z "$topic_name" || -z "$partitions" || -z "$replication_factor" ]]; then
    echo "Creating topic: $topic_name with partitions: $partitions, replication factor: $replication_factor"

    # IMPORTANT:  Quote the KAFKA_TOPICS_PATH variable here.
    if ! "$KAFKA_TOPICS_PATH" --bootstrap-server "$KAFKA_BROKERS" --create --topic "$topic_name" --partitions "$partitions" --replication-factor "$replication_factor" "${topic_configs[@]}"; then
      echo "Failed to create topic: $topic_name"
      return 1
    else
      echo "Topic $topic_name created successfully"
      return 0
    fi
  else
    echo "Topic name, partitions, and replication factor are required."
    return 1
  fi
}

# Main script logic
if [ -z "$KAFKA_BROKERS" ]; then
  echo "Error: KAFKA_BROKERS is not set.  Please set it either in the environment or in the script."
  exit 1
fi

# Check if kafka-topics.sh exists
if [ ! -x "$KAFKA_TOPICS_PATH" ]; then
  echo "Error: kafka-topics.sh not found at $KAFKA_TOPICS_PATH. Please correct the path."
  exit 1
fi

# Create the specified topics
create_topic "konfirmasi_pesanan" 1 3
create_topic "pemesan" 1 3
create_topic "order" 3 3
create_topic "notification" 1 3
create_topic "pembayaran" 1 3

echo "Topic creation script finished."
