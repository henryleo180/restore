#!/bin/bash

# Function to check for errors and exit if any
check_error() {
  if [ $? -ne 0 ]; then
    echo "Error: $1"
    exit 1
  fi
}

# Install the required packages
pip3 install -r requirements.txt
check_error "Failed to install required packages"

# Download the data
python3 download.py
check_error "Failed to download data"

# Organize the data
python3 data_organize.py
#check_error "Failed to organize data"

# Create a MySQL Docker container from mysql image
bash run_docker_mysql.sh
check_error "Failed to create a MySQL Docker container"

# Create the database and import the data
python3 db.py data/bank_data.csv
check_error "Failed to create database and import data"