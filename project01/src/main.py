import argparse
import sys
import os
from sodapy import Socrata

import requests
from requests.auth import HTTPBasicAuth
import time
from elasticsearch import Elasticsearch
from elasticsearch import helpers

DATASET_ID = os.environ["DATASET_ID"] # parking violation dataset
APP_TOKEN = os.environ["APP_TOKEN"]
ES_HOST = os.environ["ES_HOST"]
ES_USERNAME = os.environ["ES_USERNAME"]
ES_PASSWORD = os.environ["ES_PASSWORD"]
INDEX_NAME = "nycparkviolationdataset" # ES index name. Could have been passed from cmd as well

parser = argparse.ArgumentParser(description=("Process data from parking violation."))
parser.add_argument("--page_size",type=int, help="How many rows to fetch", required=True)
parser.add_argument("--num_pages",type=int, help="How many pages to fetch")
parser.add_argument("--init_offset",type=int, help="Starting offset to fetch from")
args = parser.parse_args(sys.argv[1:])

def find_num_pages():
    if args.num_pages:
        return int(args.num_pages)
    else:
        count = client.get(DATASET_ID, select='COUNT(*)')
        return int(count[0]["COUNT"]) // int(args.page_size)
     
def find_init_offset():
    if args.init_offset:
        return args.init_offset
    else:
        return 0

def create_index():
    try:
        resp = requests.put(
            f"{ES_HOST}/{INDEX_NAME}",
            auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
            json={
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                 },
                "mappings": {
                    "properties": {
        				"plate": { "type": "keyword" },
    					"state": { "type": "keyword" },
    					"license_type": { "type": "keyword" },
    					"summons_number": { "type": "keyword" },
    					"issue_date": { "type": "date", "format": "mm/dd/yyyy" },
    					"violation_time": {"type": "keyword"},
    					"violation": { "type": "keyword" },
    					"fine_amount": { "type": "float" },
    					"penalty_amount": { "type": "float" },
    					"interest_amount": { "type": "float" },
    					"reduction_amount": { "type": "float" },
    					"payment_amount": { "type": "float" },
    					"amount_due": { "type": "float" },
    					"precinct": { "type": "keyword" },
    					"county": { "type": "keyword" },
    					"issuing_agency": { "type": "keyword" }
                    }
                }
            }
        )
        resp.raise_for_status()
    except Exception as e:
        print("Index already exists!")
        
if __name__ == '__main__':
    
    # Creating ElastiSearch Client
    es_client = Elasticsearch([ES_HOST], port=9200, http_auth=(ES_USERNAME, ES_PASSWORD))
    
    # Creating Socrata Constructor
    client = Socrata(
        "data.cityofnewyork.us",
        APP_TOKEN,
        timeout=180 # Required because some offsets were taking more than 10 secs (default)
    )
    
    # Creating schema/index in ES
    create_index()
    
    # Deducing num_pages and initial offset
    num_pages = find_num_pages()
    init_offset = find_init_offset()
        
    print("Num Pages: " + str(num_pages))
    print("Init Offset: " + str(init_offset)) # Can be used to jump to the Nth offset 
    
    start_process_time = time.time()
    for page in range(0, num_pages):
        start = time.time()
        start_time_to_get_rows = time.time()
        rows = client.get(DATASET_ID, limit=args.page_size, offset=page*(args.page_size) + init_offset)
        end_time_to_get_rows = time.time()
        print("Time to make DB Call:" + str(end_time_to_get_rows - start_time_to_get_rows))
        
        final_payload = []
        for row in rows:
            try:
                es_row ={}
                es_row["plate"] = row["plate"]
                es_row["state"] = row["state"]
                es_row["summons_number"] = row["summons_number"]
                es_row["license_type"] = row["license_type"]
                es_row["issue_date"] = row["issue_date"]
                es_row["violation_time"] = row["violation_time"]
                es_row["violation"] = row["violation"]
                es_row["fine_amount"] = float(row["fine_amount"])
                es_row["penalty_amount"] = float(row["penalty_amount"])
                es_row["interest_amount"] = float(row["interest_amount"])
                es_row["reduction_amount"] = float(row["reduction_amount"])
                es_row["payment_amount"] = float(row["payment_amount"])
                es_row["amount_due"] = float(row["amount_due"])
                es_row["precinct"] = (row["precinct"])
                es_row["county"] = (row["county"])
                es_row["issuing_agency"] = (row["issuing_agency"])
                
                main = {}
                main["_index"] = INDEX_NAME
                main["_type"] = "_doc"
                main["_source"] = es_row
                
                final_payload.append(main)
                
            except Exception as e:
                # We skip failures
                # print(f"Skipping because failure: {e}")
                pass

        print("Final request being posted: "+ str(len(final_payload)))
        try:
            start_time_post = time.time()
            helpers.bulk(es_client, final_payload) # ES bulk call
            end_time_post = time.time()
            print("Time to make EC call for page : " + str(page) + " " + str(end_time_post - start_time_post))
        except Exception as e:
            print(f"Failed to upload to elasticsearcg! {e}")
        end = time.time()
        print("Page " + str(page) +  " completed in " + str(end - start) + " time")
    
    end_process_time = time.time()
    print("Process complete")  
    print("Total Time:" + str(end_process_time - start_process_time))
            

    

    