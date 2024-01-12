# 2023 Summer Internship Project

This repository contains a Python-based data analysis process for analyzing bank data. The process involves downloading data, organizing it, setting up a MySQL database, importing the data, and performing analytics.

## Prerequisites
- Python 3.x
- Docker
  
To start with, you should have Docker on your computer for data storage and for further analysis. Otherwise, we suggest that you:
1. Download Docker from [Docker Download](https://www.docker.com/products/docker-desktop/)
2. Follow install instructions.

## Usage
To get started, you need to download data and set up database first. After setting up, you can run the analytics and get the output webpage.
```
# To set up
$ ./set_up.sh

# To run analytics
$ ./run_process.sh
```