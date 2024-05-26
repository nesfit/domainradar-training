#!/usr/bin/env python3

import os
import random
import argparse
import pymongo
from pymongo import MongoClient
from dotenv import load_dotenv
import subprocess

# Load environment variables from .env file
load_dotenv()

# Use the MongoDB URI from the .env file
mongo_uri = os.getenv('DR_MONGO_URI')
db_name = os.getenv('DR_DB_NAME', 'drdb')

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client[db_name]

def is_domain_live(domain, timeout, verbose):
    if verbose:
        print(f"Checking if domain {domain} is live with timeout {timeout}...")
    try:
        result = subprocess.run(['ping', '-c', '1', '-W', str(timeout), domain], stdout=subprocess.DEVNULL)
        live = result.returncode == 0
        if verbose:
            print(f"Domain {domain} is {'live' if live else 'not live'}.")
        return live
    except Exception as e:
        if verbose:
            print(f"Error checking domain {domain}: {e}")
        return False

def main(source, target, n, livecheck, exclude=None, timeout=1, verbose=True):
    if verbose:
        print("Connecting to MongoDB...")
    
    source_collection = db[source]
    target_collection = db[target]

    exclude_domains = set()
    if exclude:
        exclude_collection = db[exclude]
        exclude_domains = set(doc['domain_name'] for doc in exclude_collection.find({}, {'domain_name': 1}))
        if verbose:
            print(f"Loaded {len(exclude_domains)} domains from exclude collection.")

    candidates = [doc for doc in source_collection.find({'domain_name': {'$nin': list(exclude_domains)}})]
    random.shuffle(candidates)
    if verbose:
        print(f"Found {len(candidates)} candidate domains in source collection.")
    
    if len(candidates) < n:
        print(f"Warning: Not enough candidates available. Needed: {n}, Found: {len(candidates)}")

    selected_domains = []
    excluded_domains = len(exclude_domains)
    nonlive_domains = 0

    for index, candidate in enumerate(candidates):
        if len(selected_domains) >= n:
            break
        if candidate['domain_name'] not in exclude_domains:
            if livecheck:
                if is_domain_live(candidate['domain_name'], timeout, verbose):
                    selected_domains.append(candidate)
                else:
                    nonlive_domains += 1
            else:
                selected_domains.append(candidate)

        if verbose:
            processed = index + 1
            remaining = len(candidates) - processed
            print(f"Processed: {processed}, Selected: {len(selected_domains)}, Non-live: {nonlive_domains}, Remaining: {remaining}")

    if verbose:
        print(f"Inserting {len(selected_domains)} domains into target collection...")
    if selected_domains:
        target_collection.insert_many(selected_domains)

    print(f"Selected domains: {len(selected_domains)}")
    print(f"Excluded domains: {excluded_domains}")
    print(f"Non-live domains: {nonlive_domains}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Select and insert documents into a target MongoDB collection.')
    parser.add_argument('source', type=str, help='The name of the source collection.')
    parser.add_argument('target', type=str, help='The name of the target collection.')
    parser.add_argument('n', type=int, help='The number of desired rows.')
    parser.add_argument('--exclude', type=str, help='The name of the exclude collection (optional).')
    parser.add_argument('--livecheck', action='store_true', help='Check if the domain names are live.')
    parser.add_argument('--timeout', type=int, default=1, help='Specify the ping timeout in seconds (default is 1).')
    parser.add_argument('--verbose', type=int, choices=[0, 1], default=1, help='Enable verbose mode (default is 1).')

    args = parser.parse_args()
    main(args.source, args.target, args.n, args.livecheck, args.exclude, args.timeout, args.verbose == 1)
