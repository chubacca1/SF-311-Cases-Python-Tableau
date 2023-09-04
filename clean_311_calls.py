#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 2 22:26:47 2023

@author: eric
"""

import pandas as pd

month_mapping = {
    'Jan': 'January',
    'Feb': 'February',
    'Mar': 'March',
    'Apr': 'April',
    'May': 'May',
    'Jun': 'June',
    'Jul': 'July',
    'Aug': 'August',
    'Sep': 'September',
    'Oct': 'October',
    'Nov': 'November',
    'Dec': 'December'
}

data = pd.read_csv('311_Call_Metrics_by_Month.csv')

# Extract Month and Year into separate columns
data['Year'] = data['Month'].str.split().str[1]
data['Month'] = data['Month'].str.split().str[0].map(month_mapping)

# Convert year column to int and filter out specific years
data['Year'] = data['Year'].astype(int)
data = data[(data['Year'] >= 2020) & (data['Year'] <= 2022)]

# Reorder the columns so that year is next to month
reorderd = ['Month', 'Year', 'Calls Answered', 'Svc Level (% answered w/i 60 sec)',
       'Avg Speed Answer (sec)', 'Transferred Calls %']
data = data[reorderd]

# Export to new CSV file
data.to_csv('311_call_metrics_cleaned.csv', index = False)