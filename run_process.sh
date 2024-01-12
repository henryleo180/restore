#!/bin/bash
# Function to check for errors and exit if any
check_error() {
  if [ $? -ne 0 ]; then
    echo "Error: $1"
    exit 1
  fi
}

# Get container name
CONTAINER_NAME=$(sed -n "s/CONTAINER_NAME=//p" config.ini)

# Restart the mysql container on Docker
docker restart ${CONTAINER_NAME}

sleep 5;

# Run the analytics
echo "Now open the website..."
python3 analytics.py
check_error "Failed to run analytics"

echo "Exit Bank Analysis Program"
exit 0
