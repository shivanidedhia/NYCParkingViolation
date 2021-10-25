# NYC Open Parking and Camera Violation Dashboard

## Description
The NYC Open Parking and Camera Violation Dashboard, the dataset has 71.55 million rows and 19 columns as of Oct 2021. Each row is an open parking and camera violations issued in New York city traced back from 2000 to now.

- Language
  - Python: sodapy, sys, argparse, json, requests, os, elasticsearch, datetime, time
  - Docker
- Service: ElasticSearch, Kibana

## Steps to load into Elasticsearch

Step 1: Build the docker image
```
docker build -t bigdata1:1.0 project01/
````

Step 2: Run the docker container 
```
docker run -v ${PWD}:/app -e DATASET_ID=“XXX” -e APP_TOKEN=“XXX” -e ES_HOST=“XXX” -e ES_USERNAME=“XXX” -e ES_PASSWORD=“XXX” bigdata1:1.0 --page_size=1000 

OPTIONAL --num_pages=1000, --init_offset=0
```

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
