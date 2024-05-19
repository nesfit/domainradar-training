#!/usr/bin/env python3

import os
import sys
import argparse
import pandas as pd
import pyarrow.parquet as pq

from transformers.lexical import lex

def parse_args():
    parser = argparse.ArgumentParser(description='Process domain names from a file or directory.')
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--inputfile', type=str, help='Path to the input file containing domain names.')
    group.add_argument('--inputdir', type=str, help='Path to the input directory containing text files with domain names.')
    
    parser.add_argument('--label', type=str, default='unknown', help='Label for the domain names. Default is "unknown".')
    parser.add_argument('--output-parquet', type=str, help='Output filename for the Parquet file.')
    parser.add_argument('--output-csv', type=str, help='Output filename for the CSV file.')
    parser.add_argument('--csv-delimiter', type=str, default=';', help='Delimiter for the CSV file. Default is ";".')
    parser.add_argument('--output-json', type=str, help='Output filename for the JSON file.')
    
    args = parser.parse_args()
    
    if not (args.output_parquet or args.output_csv or args.output_json):
        parser.error('At least one output method (and filename) must be specified.')
    
    return args

def read_domains_from_file(file_path):
    with open(file_path, 'r') as file:
        return file.read().splitlines()

def read_domains_from_dir(dir_path):
    domains = []
    for filename in os.listdir(dir_path):
        if filename.endswith('.txt'):
            file_path = os.path.join(dir_path, filename)
            domains.extend(read_domains_from_file(file_path))
    return domains

def main():
    args = parse_args()
    
    if args.inputfile:
        domains = read_domains_from_file(args.inputfile)
    elif args.inputdir:
        domains = read_domains_from_dir(args.inputdir)
    
    if not domains:
        print("No domains found in the input.")
        sys.exit(1)
    
    df = pd.DataFrame(domains, columns=['domain_name'])
    df['label'] = args.label
    
    df = lex(df)
    
    if args.output_parquet:
        df.to_parquet(args.output_parquet)
    
    if args.output_csv:
        df.to_csv(args.output_csv, sep=args.csv_delimiter, index=False)
    
    if args.output_json:
        df.to_json(args.output_json, orient='records', lines=True)
    
    if args.label == 'unknown':
        print("Warning: The label was not specified. Defaulting to 'unknown'.")

if __name__ == "__main__":
    main()
