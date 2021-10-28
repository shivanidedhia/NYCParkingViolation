# NYC Open Parking and Camera Violation Dashboard

## Description
The NYC Open Parking and Camera Violation Dashboard, the dataset has 71.55 million rows and 19 columns as of Oct 2021. Each row is an open parking and camera violations issued in New York city traced back from 2000 to now.

## File Manifest

project01/

+-- Dockerfile

+-- requirements.txt

+-- src/

+-- +-- main.py

+-- assets/

+-- +-- kibanadashboard.png

+-- README

## Tech/Framework used

```
- Docker
- Service: ElasticSearch, Kibana
- Python: sodapy, sys, argparse, json, requests, os, datetime, time
```

## Features

Elasticsearch BULK API is used to decrease time taken to load data into Elasticsearch.
Making it possible to perform many write operations in a single API call. This makes it more efficient than sending multiple bulk requests.  

## Steps to load into Elasticsearch

Step 1: Build the docker image
```
docker build -t bigdata1:1.0 project01/
````

Step 2: Run the docker container 
```	
docker run -e DATASET_ID="nc67-uf89" -e APP_TOKEN=" " -e ES_HOST=  -e ES_USERNAME=" " -e ES_PASSWORD=" " -e INDEX_NAME="park" bigdata1:1.0 --num_pages=3 --page_size=2

```

Step 3: Optional Arguments 

```
--num_pages=10, --init_offset=0

```

## API reference

[Open Parking and Camera Violations](https://dev.socrata.com/foundry/data.cityofnewyork.us/nc67-uf89)

## Visualized the data in Kibana

1. Most Popular Violation Time

- 8:36AM was the time when most violations occured. 

2. Total Fine Amount

- More than 9 million fines were between $30 to $60.

3. Number of Violations Loaded into Kibana

4. Top 5 Violation Type

- Top violation type was school zone speeding
- Second was parking on street cleaning day

<img src="/project01/assets/kibanadashboard.png" width=1000>
