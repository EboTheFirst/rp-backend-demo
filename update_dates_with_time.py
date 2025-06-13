#!/usr/bin/env python3
"""
Script to add time components to the date column in transactions.csv
This will modify dates from YYYY-MM-DD format to YYYY-MM-DD HH:MM:SS format
"""

import pandas as pd
import numpy as np
from datetime import datetime, time
import random

def generate_realistic_time():
    """Generate realistic transaction times with business hour bias"""
    # Weight towards business hours (8 AM to 8 PM)
    hour_weights = [
        0.5,  # 00:00 - very low
        0.3,  # 01:00 - very low
        0.2,  # 02:00 - very low
        0.1,  # 03:00 - very low
        0.1,  # 04:00 - very low
        0.2,  # 05:00 - very low
        0.5,  # 06:00 - low
        1.0,  # 07:00 - low
        3.0,  # 08:00 - business hours start
        4.0,  # 09:00 - high
        5.0,  # 10:00 - high
        5.5,  # 11:00 - high
        6.0,  # 12:00 - peak (lunch)
        5.5,  # 13:00 - high
        5.0,  # 14:00 - high
        4.5,  # 15:00 - high
        4.0,  # 16:00 - high
        4.5,  # 17:00 - high (after work)
        5.0,  # 18:00 - high (evening)
        4.0,  # 19:00 - moderate
        3.0,  # 20:00 - moderate
        2.0,  # 21:00 - low
        1.5,  # 22:00 - low
        1.0,  # 23:00 - low
    ]
    
    # Choose hour based on weights
    hour = np.random.choice(24, p=np.array(hour_weights) / sum(hour_weights))
    
    # Generate random minutes and seconds
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    
    return f"{hour:02d}:{minute:02d}:{second:02d}"

def update_csv_with_times():
    """Update the transactions.csv file to include time components"""
    
    print("Loading transactions.csv...")
    
    # Read the CSV file
    df = pd.read_csv('data/transactions.csv')
    
    print(f"Loaded {len(df)} transactions")
    print(f"Current date format sample: {df['date'].head().tolist()}")
    
    # Set random seed for reproducibility
    np.random.seed(42)
    random.seed(42)
    
    print("Generating time components...")
    
    # Generate time components for each transaction
    times = []
    for i in range(len(df)):
        if i % 10000 == 0:
            print(f"Processing transaction {i+1}/{len(df)}")
        times.append(generate_realistic_time())
    
    # Combine date and time
    print("Combining dates with times...")
    df['date'] = df['date'].astype(str) + ' ' + pd.Series(times)
    
    print(f"Updated date format sample: {df['date'].head().tolist()}")
    
    # Create backup of original file
    print("Creating backup of original file...")
    df_original = pd.read_csv('data/transactions.csv')
    df_original.to_csv('data/transactions_backup.csv', index=False)
    
    # Save updated file
    print("Saving updated transactions.csv...")
    df.to_csv('data/transactions.csv', index=False)
    
    print("✅ Successfully updated transactions.csv with time components!")
    print(f"✅ Original file backed up as transactions_backup.csv")
    print(f"✅ Updated {len(df)} transaction records")
    
    # Show some statistics
    print("\n📊 Time Distribution Sample:")
    sample_times = pd.to_datetime(df['date'].head(100))
    hour_dist = sample_times.dt.hour.value_counts().sort_index()
    print("Hour distribution (first 100 records):")
    for hour, count in hour_dist.head(10).items():
        print(f"  {hour:02d}:xx - {count} transactions")

if __name__ == "__main__":
    try:
        update_csv_with_times()
    except FileNotFoundError:
        print("❌ Error: transactions.csv not found in data/ directory")
        print("Please make sure you're running this script from the rpay-analytics-api directory")
    except Exception as e:
        print(f"❌ Error: {str(e)}")
