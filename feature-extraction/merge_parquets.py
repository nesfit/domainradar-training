#!/usr/bin/env python3

import sys
import argparse
import pandas as pd
import pyarrow.parquet as pq

def merge_parquet_files(input_files, output_file, shuffle=False):
    # Load the first file to get column names and types
    first_file = input_files[0]
    df_first = pd.read_parquet(first_file)

    # Initialize merged DataFrame
    merged_df = df_first

    # Merge the rest of the files
    for file in input_files[1:]:
        df = pd.read_parquet(file)
        
        # Check if column names match
        if set(df.columns) != set(df_first.columns):
            print(f"Error: Column names do not match in file {file}")
            sys.exit(1)
        
        # Rearrange columns if necessary
        df = df[df_first.columns]

        # Append to merged DataFrame
        merged_df = pd.concat([merged_df, df], ignore_index=True)

    # Shuffle rows if specified
    if shuffle:
        merged_df = merged_df.sample(frac=1).reset_index(drop=True)

    # Write merged DataFrame to parquet file
    merged_df.to_parquet(output_file)

    print(f"Merged files saved to {output_file}")

def display_usage():
    usage = """
    Usage: merge_parquets.py --input <file1,file2,...> --output <output_file> [--shuffle]

    Example:
    ./merge_parquets.py --input file1.parquet,file2.parquet --output merged.parquet --shuffle
    """
    print(usage)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge Parquet files")
    parser.add_argument("--input", required=True, help="Input Parquet files separated by comma")
    parser.add_argument("--output", required=True, help="Output Parquet file")
    parser.add_argument("--shuffle", action="store_true", help="Shuffle rows (optional)")

    if len(sys.argv) < 2 or "--help" in sys.argv or "-h" in sys.argv:
        display_usage()
        sys.exit(0)

    args = parser.parse_args()

    input_files = args.input.split(",")
    output_file = args.output
    shuffle = args.shuffle

    merge_parquet_files(input_files, output_file, shuffle)
