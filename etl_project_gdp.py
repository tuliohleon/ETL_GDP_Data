from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''

    #Extract the web page as text
    html_page = requests.get(url).text

    #Parse the html page    
    data = BeautifulSoup(html_page, 'html.parser')

    #Create an empty pandas DataFrame named df with columns as the table_attribs
    df = pd.DataFrame(columns=table_attribs)

    #Extract all 'tbody' attributes of the HTML object and then extract all the rows of the index 2 table using the 'tr' attribute.
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')

    #Check the contents of each row, having attribute ‘td’, for the following conditions.
    #a. The row should not be empty.
    #b. The first column should contain a hyperlink.
    #c. The third column should not be '—'.

    rows_to_add = []

    for row in rows:
            # Extract all 'td' elements in the current row
        cells = row.find_all('td')

            # Check conditions: non-empty row, first column contains a hyperlink, third column is not '—'
        if len(cells) > 2 and cells[0].find('a') and cells[2].text.strip() != '—':
                
                # Extract data for each attribute
            country = cells[0].find('a').text.strip()
            gdp_usd_millions = cells[2].text.strip().replace(',', '').replace('$', '')

                # Store valid entries in a dictionary
            entry = {
                table_attribs[0]: country,
                table_attribs[1]: float(gdp_usd_millions) if gdp_usd_millions else None
                }

                # Add the entry to the list of rows to add
            rows_to_add.append(entry)
        # Concatenate all rows to the DataFrame
    df = pd.concat([df, pd.DataFrame(rows_to_add)], ignore_index=True)

    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''

    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ["Country", "GDP_USD_millions"]
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'

df=extract(url, table_attribs)