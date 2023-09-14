#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 2 13:42:15 2023

@author: eric
"""

import pandas as pd
import os


# Globals

MONTHS = {
    1: 'January',
    2: 'February',
    3: 'March',
    4: 'April',
    5: 'May',
    6: 'June',
    7: 'July',
    8: 'August',
    9: 'September',
    10: 'October',
    11: 'November',
    12: 'December'
}

# The large 2.8gb CSV file of non emergency cases made to 311 from data.sfgov.org
LARGE_CSV = '311_cases.csv'

# The new folder name to store the chunked CSV files
SPLIT_CSV_DIR = '311_cases_year'

# The new smaller CSV file
COMBINED_CSV = '311_cases_subset.csv'

# The years of interest from when Covid began to the end of 2022
YEARS_TO_COMBINE = [2020, 2021, 2022]

# Original CSV file contained lots of unnecessary columns
# Only keep what may be needed for now and can do further cleaning later
COLUMNS_TO_KEEP = [
    'CaseID',
    'Opened',
    'Closed',
    'Updated',
    'Status',
    'Status Notes',
    'Responsible Agency',
    'Category',
    'Request Type',
    'Request Details',
    'Neighborhood',
    'Latitude',
    'Longitude',
    'Point',
    'Source'
]


def split_csv_by_year(input_csv, output_dir):
    """
    Split a large CSV file into smaller files by year.

    Args:
        input_csv (str): The path to the CSV file.
        output_dir (str): The directory where the split CSV files will be saved.
    """
    try:
        # Make a new folder to store split csv files
        os.makedirs(output_dir, exist_ok=True)
        
        # Split csv into chunks for faster reading
        chunk_size = 10000
        
        for chunk in pd.read_csv(input_csv, chunksize=chunk_size):
            
            # Format the 'Opened'/'Closed'/'Updated' column dates for easier parsing
            date_format = "%m/%d/%Y %I:%M:%S %p"
            chunk['Opened'] = pd.to_datetime(chunk['Opened'], format=date_format)
            chunk['Closed'] = pd.to_datetime(chunk['Closed'], format=date_format)
            chunk['Updated'] = pd.to_datetime(chunk['Updated'], format=date_format)
            
            # Sort files based off of 'Opened' year
            for year, group in chunk.groupby(chunk['Opened'].dt.year):
                # Create the output directory for the currently checked year
                year_dir = os.path.join(output_dir, str(year))
                os.makedirs(year_dir, exist_ok=True)
                
                # Generate the output CSV file name
                output_csv = os.path.join(year_dir, f'data_{year}_{len(group)}.csv')
                
                # Write the chunk for the currently checked year to the CSV file
                group.to_csv(output_csv, index=False)
    except Exception as e:
        print(f"Error occurred while splitting CSV by year: {str(e)}")


def combine_csv_by_years(input_dir, output_csv, years_to_combine, columns_to_keep):
    """
    Combine CSV files from specified years into a single CSV file.

    Args:
        input_dir (str): The directory containing the split CSV files.
        output_csv (str): The path to the output combined CSV file.
        years (int): Year to combine
        columns_to_keep (list): List of column to keep in the final CSV.
    """
    try:
        # Construct the dataframe of the columns we want
        combined_data = pd.DataFrame(columns=columns_to_keep)

        # Grab the specific year folder
        for year in years_to_combine:

            year_dir = os.path.join(input_dir, str(year))
            
            # Loop through the chunked CSV in the year folder
            for filename in os.listdir(year_dir):
                if filename.endswith(".csv"):
                    csv_file = os.path.join(year_dir, filename)
                    chunk = pd.read_csv(csv_file)
    
                    # Concat only the specified columns data
                    chunk = chunk[columns_to_keep]
                    combined_data = pd.concat([combined_data, chunk], ignore_index=True)
    
            # Save the combined data to the output CSV
            combined_data.to_csv(output_csv, index=False)

    except Exception as e:
        print(f"Error occurred while combining CSV files by years: {str(e)}")


def clean_csv_for_visualization(combined_csv_file):
    """
    Cleans and processes a combined CSV file containing 311 cases data for visualization.

    Args:
        combined_csv_file (str): The path to the combined CSV file to be cleaned.

    This function reads the combined CSV file, converts date columns to datetime objects,
    calculates time-related metrics, adds columns for the opened month and year, and exports
    the cleaned data to a new CSV file.
    """
    data = pd.read_csv(combined_csv_file)

    datetime_format = '%Y-%m-%d %H:%M:%S'
    
    # Convert 'Opened', 'Closed', and 'Updated' columns to datetime objects
    date_columns = ['Opened', 'Closed', 'Updated']
    for col in date_columns:
        data[col] = pd.to_datetime(data[col], format=datetime_format, errors='coerce')

    # Calculate the time to close a case in days if 'Closed' data is available
    data['Time_to_Close'] = (data['Closed'] - data['Opened']).dt.total_seconds() / (60 * 60 * 24)
    
    # Calculate the time to update a case in days if 'Updated' data is available
    data['Time_to_Update'] = (data['Updated'] - data['Opened']).dt.total_seconds() / (60 * 60 * 24)

    # Adding a column to indicate the month and year the case was opened
    # Allows Tableau to filter and groupby
    data['Opened_Month'] = data['Opened'].dt.month.map(MONTHS)
    data['Opened_Year'] = data['Opened'].dt.year
    
    # export into csv
    data.to_csv('311_cases_subset_cleaned.csv', index = False)



if __name__ == "__main__":
    # Call function to split the large CSV file by year
    split_csv_by_year(LARGE_CSV, SPLIT_CSV_DIR)
    
    # Combine the CSV files by year
    combine_csv_by_years(SPLIT_CSV_DIR, COMBINED_CSV, YEARS_TO_COMBINE, COLUMNS_TO_KEEP)
    
    # Clean some of the data for visualization in Tableau
    clean_csv_for_visualization(COMBINED_CSV)
