import sys
import os
import argparse
import json
import threading
import time
import requests
import datetime

from time import time
from sodapy import Socrata
from datetime import datetime, date
from math import ceil

from config import mappings as es_mappings
from elastic_helper import (
    ElasticHelperException,
    insert_doc,
    try_create_index,
)

WEBSITE = "data.cityofnewyork.us"
DATASET_ID = os.environ.get("DATASET_ID")
APP_TOKEN = os.environ.get("APP_TOKEN")
ES_HOST = os.environ.get("ES_HOST")
ES_USERNAME = os.environ.get("ES_USERNAME")
ES_PASSWORD = os.environ.get("ES_PASSWORD")

def boost(id_, page_size, offset):
    x1 = time()
    resp = client.get(id_, limit=page_size, offset=offset)
    print(f"Finished in: {time()-x1}")
    
    with open('output.txt', 'a+') as fh:
        for item in resp:
            fh.write(f"{str(item)}\n")

if __name__ == '__main__':
    """
    args = sys.argv[1:]
    
    limit = int(args[0])
    try:
        offset = int(args[1])
    except:
        offset = 0
    """    
    client = Socrata(
        WEBSITE,
        APP_TOKEN,
    )
    # run a query to count number of rows in dataset
    results = client.get(DATASET_ID, select='COUNT(*)')
    total = int(results[0]['COUNT'])
    parser = argparse.ArgumentParser()
    parser.add_argument('--page_size', type=int, help='how many rows to get per page', required=True)
    parser.add_argument('--num_pages', type=int, help='how many pages to get in total', required=False)
    args = parser.parse_args()
    page_size = args.page_size
    num_pages = args.num_pages
    
    
    x0 = time()
    threads = []
    for i in range(num_pages):
        t = threading.Thread(
            target=boost,
            args=(DATASET_ID, page_size, i*page_size, ),
        )
        threads.append(t)
        t.start()
        
    for th in threads:
        th.join()
    print(f"Done {time()-x0}")

    try:
        try_create_index(
            "project01dani", 
            host=ES_HOST,
            mappings=es_mappings,
            es_user=ES_USERNAME,
            es_pw=ES_PASSWORD,
        )
    except ElasticHelperException as e:
        print(f"Index already exists! Skipping. Reason: {e}")
        
    #Quering data to get rows
    rows = client.get(DATASET_ID, limit=page_size, offset=num_pages, order=":id")
    
    
    #Converting row data into the correct type as needed
    for row in rows:
        try:
            #row['penalty_amount'] = float(row['penalty_amount'])
            #row['fine_amount'] = float(row['fine_amount'])
            #row['summons_number'] = float(row['summons_number'])
            #row['interest_amount'] = float(row['interest_amount'])
            #row['reduction_amount'] = float(row['reduction_amount'])
            #row['payment_amount'] = float(row['payment_amount'])
            #row['amount_due'] = float(row['amount_due'])
            row['issue_date'] = str(datetime.strptime(row['issue_date'],'%m/%d/%Y').date())
        except Exception as e:
            print(f"SKIPPING! Failed to transform row: {row}. Reason: {e}")
            continue
        
    #Posting data to elasticsearch
        try:
            ret = insert_doc(
                "project01dani", 
                host=ES_HOST,
                data=row,
                es_user=ES_USERNAME,
                es_pw=ES_PASSWORD,
            )
            print(ret)
        except ElasticHelperException as e:
            print(e)