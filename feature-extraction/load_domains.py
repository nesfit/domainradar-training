#!/usr/bin/env python3

import os
import sys
from pymongo import MongoClient
from dotenv import load_dotenv
import argparse

# Load environment variables from .env file
load_dotenv()

# Use the MongoDB URI from the .env file
mongo_uri = os.getenv('DR_MONGO_URI')
db = os.getenv('DR_DB_NAME', 'drdb')

def main():
    parser = argparse.ArgumentParser(description='Process and manipulate domain names from a MongoDB collection.')
    parser.add_argument('collection_name', type=str, nargs='?', help='Name of the MongoDB collection')
    parser.add_argument('output_file', type=str, nargs='?', help='Output file to store the domains')
    parser.add_argument('--union', action='store_true', help='Add only new domains to the output file')
    parser.add_argument('--intersect', action='store_true', help='Intersect domains in file and MongoDB collection')
    parser.add_argument('--file-only', action='store_true', help='Output file domains not in MongoDB collection')
    parser.add_argument('--mongo-only', action='store_true', help='Output MongoDB collection domains not in file')

    args = parser.parse_args()

    # Display help and exit if required arguments are not provided
    if not args.collection_name or not args.output_file:
        parser.print_help()
        sys.exit(1)

    # Connect to MongoDB and access the specified database and collection
    client = MongoClient(mongo_uri)
    database = client[db]  # Corrected this line
    collection = database[args.collection_name]  # Corrected this line

    # Extract domain names from MongoDB collection
    mongo_domains = set(document.get('domain_name', '') for document in collection.find({}, {'domain_name': 1}))

    # Read existing domains from file if it exists
    if os.path.exists(args.output_file):
        with open(args.output_file, 'r') as file:
            file_domains = set(line.strip() for line in file)
    else:
        file_domains = set()

    # Determine action based on arguments
    if args.union:
        final_domains = file_domains.union(mongo_domains)
    elif args.intersect:
        final_domains = file_domains.intersection(mongo_domains)
    elif args.file_only:
        final_domains = file_domains - mongo_domains
    elif args.mongo_only:
        final_domains = mongo_domains - file_domains
    else:
        final_domains = mongo_domains

    # Write final domain list to file
    with open(args.output_file, 'w') as file:
        for domain in sorted(final_domains):
            file.write(domain + '\n')

    print(f'Domains have been processed and saved to {args.output_file}')


if __name__ == '__main__':
    main()
