# This script performs an ETL (Extract, Transform, Load) operation on banking data.
# Extract: Scrapes data from a web archive of the largest banks.
# Transform: Converts market capitalization to different currencies using exchange rates.
# Load: Saves the processed data into a CSV file and a SQLite database.

#Author: Tooba Javed


# # Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd 
import sqlite3
import numpy as np 
from datetime import datetime

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_rate_csv = './exchange_rate.csv'
csv_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
table_attribs = ['Name',
                  'MC_USD_Billion', 
                  'MC_GBP_Billion', 
                  'MC_EUR_Billion', 
                  'MC_INR_Billion']
log_file = './code_log.txt'

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%m-%d %H:%M:%S'  # Year-Month-Day Hour:Minute:Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ' : ' + message + '\n')

log_progress('Preliminaries complete. Initiating ETL process')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    # Extract the HTML content of the webpage
    html_page = requests.get(url).text
    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(html_page, 'html.parser')
    data = []
    # Find all the tables present in the parsed HTML
    tables = soup.find_all('tbody')
    # Retrieving all the rows of the index 0(first) table using the tr attribute
    rows = tables[0].find_all('tr')
    for row in rows:
        # Extract all columns in the row
        cols = row.find_all('td')
        #Iterate through each row to extrafct the bank name and market cap 
        if len(cols) > 1:
            name = cols[1].text.strip()
            mc_usd_billion = cols[2].text.strip().\
                replace('\n', '').replace(',', '')
            if mc_usd_billion[-1] == 'B':
                mc_usd_billion = float(mc_usd_billion[:-1])
            else:
                mc_usd_billion = float(mc_usd_billion)
            data.append([name, mc_usd_billion])
    
    df = pd.DataFrame(data, columns=table_attribs[:2])
    return df

df = extract(url, table_attribs)
print(df)
log_progress('Data extraction complete. Initiating Transformation process')

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    # Load exchange rate data from the CSV file into a DataFrame
    exchange_rate_df = pd.read_csv(exchange_rate_csv)
    # Convert the exchange rate DataFrame to a dictionary
    exchange_rate = exchange_rate_df.set_index('Currency')['Rate'].to_dict()
    
    # Transform market cap in USD to GBP, EUR, and INR using exchange rates
    # and round the results to 2 decimal places    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]
    return df

df = transform(df, exchange_rate_csv)
print(df)
print(df['MC_EUR_Billion'][4])
log_progress('Data transformation complete. Initiating Loading process')

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

# logging that the data has been loaded in the output file (CSV)
log_progress('Data loaded to CSV file')
load_to_csv(df, csv_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as a table, Executing queries')

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    cursor = sql_connection.cursor()
    cursor.execute(query_statement)
    # Execute the SQL query and fetch all results
    results = cursor.fetchall()
    # Print the results of the SQL query for inspection
    print(f"\nQuery: {query_statement}")
    for row in results:
        print(row)

run_query('SELECT * FROM Largest_banks', sql_connection)
run_query('SELECT AVG(MC_GBP_Billion) FROM Largest_banks', sql_connection)
run_query('SELECT Name FROM Largest_banks LIMIT 5', sql_connection)

log_progress('ETL process complete.')

sql_connection.close()

log_progress('Server Connection closed')
