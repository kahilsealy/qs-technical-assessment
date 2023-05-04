#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sqlite3
import pandas as pd
import glob
import re
import json 
import requests
import time

def printnow(x):
    print(time.strftime('%H:%M%:%S'),str(x))
    
printnow('START')


# In[2]:


ny_transactions = pd.read_csv('./qs-analytics-engineering-exercise-v1/data/ny.csv.gz',index_col=0)
print(ny_transactions.shape)
ny_transactions.head()


# In[3]:


# Add transactions country identifier as integer to reduce storage
ny_transactions['bar_no'] = 3


# In[4]:


## Checked for data dtypes and missing values
# print(ny_transactions.dtypes)
# ny_transactions.isna().sum()


# In[5]:


budapest_transactions = pd.read_csv('./qs-analytics-engineering-exercise-v1/data/budapest.csv.gz',index_col=0)
print(budapest_transactions.shape)
budapest_transactions.head()


# In[6]:


# Add transactions country identifier as integer to reduce storage
budapest_transactions['bar_no'] = 1


# In[7]:


## Checked for data dtypes and missing values
# print(budapest_transactions.dtypes)
# budapest_transactions.isna().sum()


# In[8]:


london_transactions = pd.read_csv('./qs-analytics-engineering-exercise-v1/data/london_transactions.csv.gz',
                                  header=None,sep='\t',index_col=0)
print(london_transactions.shape)
london_transactions.head()


# In[9]:


london_transactions.index.name = None


# In[10]:


# Add transactions country identifier as integer to reduce storage
london_transactions['bar_no'] = 2


# In[11]:


london_transactions.head()


# In[12]:


## Checked for data dtypes and missing values
# print(london_transactions.dtypes)
# london_transactions.isna().sum()


# In[13]:


stock_data = pd.read_csv('./qs-analytics-engineering-exercise-v1/data/bar_data.csv')
print(stock_data.shape)
stock_data.head()


# In[14]:


# Check dtypes and check for missing values
print(stock_data.dtypes)
stock_data.isna().sum()


# In[15]:


# Remove all non-numeric characters and convert to int - one row has 34 glasses
stock_data['stock'] = stock_data['stock'].apply(lambda x: int(re.sub(r'\D','',x)))


# In[16]:


stock_data.dtypes


# In[17]:


# Rename columns of transactions dataframes to be aligned
transactiondfs = [budapest_transactions,ny_transactions,london_transactions]

cols = ['timestamp','drink','amount','bar_no']
for df in transactiondfs:
    df.columns = cols


# In[18]:


for df in transactiondfs:
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['date_created'] = df['timestamp'].dt.date
    df = df.drop(columns=['timestamp'])


# In[19]:


# Group transactions data into daily data to reduce data overhead as bars are 24 hour with consistent traffic
london_transacs = london_transactions.groupby(['date_created','drink','bar_no'])[['amount']].agg([('total_amount','sum'),
                                                                      ('drinks_sold','count')]).droplevel(0,axis=1).reset_index()

budapest_transacs = budapest_transactions.groupby(['date_created','drink','bar_no'])[['amount']].agg([('total_amount','sum'),
                                                                      ('drinks_sold','count')]).droplevel(0,axis=1).reset_index()

ny_transacs = ny_transactions.groupby(['date_created','drink','bar_no'])[['amount']].agg([('total_amount','sum'),
                                                                      ('drinks_sold','count')]).droplevel(0,axis=1).reset_index()


# In[20]:


# URL for searching for cocktail on Cocktail DB
baseurl = "http://www.thecocktaildb.com/api/json/v1/1/search.php?s="

# Function to return the glass type for a given cocktail
def return_cocktaildb_data(url,drink):
    
    url = url + drink
    data = requests.get(url)
    res = json.loads(data.text)
    
    glass_type = res['drinks'][0]['strGlass']
    glass_type = glass_type.lower()
    return glass_type


# In[21]:


# Get unique list of all the drinks served in the bars
drinkslst = budapest_transactions['drink'].unique().tolist()  + london_transactions['drink'].unique().tolist() + ny_transactions['drink'].unique().tolist()
drinkslst = list(dict.fromkeys(drinkslst))


# In[22]:


# Iterate through list of all unique drinks sold in chain of bars and create dictionary with k,v of drink
# and glass type
printnow('START - GET API DATA')

cocktail_glasses = {}
for drink in drinkslst:
    cocktail_glasses[drink] = return_cocktaildb_data(baseurl,drink)
    
printnow('END - GET API DATA')


# In[23]:


cocktail_glass_typedf = pd.DataFrame.from_dict(cocktail_glasses,orient='index').reset_index().rename(columns={'index':'drink',0:'glass_type'})


# In[25]:


barlst = [(1,'budapest'),
         (2,'london'),
         (3,'new york')]

bardf = pd.DataFrame(barlst,columns=['bar_no','bar'])


# In[26]:


def create_database_tables(sqlfile):
    
    # Connect to database
    conn = sqlite3.connect('highend_bar.db')
    
    # Create cursor object to execute SQL queries
    cur = conn.cursor()
    
    # Open the .sql file and read the contents
    with open(sqlfile, 'r') as f:
        sql_script = f.read()
    
    try:
        cursor_obj = cur.executescript(sql_script)
         # Commit the changes and close the connection
        conn.commit()
        conn.close()
        print('Script executed successfully')
        
    except sqlite3.Error as e:
        print(f'Script failed to execute: {e}')
        conn.close()


# In[27]:


create_database_tables('data_tables.sql')


# In[28]:


def dataframe_to_table(df,table_name):
    
    conn = sqlite3.connect('highend_bar.db')

    tbl_obj = df.to_sql(table_name, conn, if_exists='append', index=False)
    
    if tbl_obj:
        print('DataFrame successfully inserted')
        
    else:
        print('DataFrame not successfully inserted')


# In[29]:


# Insert DataFrame data to respective SQL table
dataframe_to_table(london_transacs,'london_daily_transactions')


# In[30]:


# Insert DataFrame data to respective SQL table
dataframe_to_table(budapest_transacs,'budapest_daily_transactions')


# In[31]:


# Insert DataFrame data to respective SQL table
dataframe_to_table(ny_transacs,'new_york_daily_transactions')


# In[32]:


# Insert DataFrame data to respective SQL table
dataframe_to_table(bardf,'bar_locations')


# In[33]:


# Insert DataFrame data to respective SQL table
dataframe_to_table(stock_data,'glass_stocks_all_bars')


# In[34]:


# Insert DataFrame data to respective SQL table
dataframe_to_table(cocktail_glass_typedf,'glass_types_all_cocktails')


# In[35]:


def creat_poc_tables(sqlfile):
    
    # Connect to database
    conn = sqlite3.connect('highend_bar.db')
    
    # Create cursor object to execute SQL queries
    cur = conn.cursor()
    
    # Open the .sql file and read the contents
    with open(sqlfile, 'r') as f:
        sql_script = f.read()
    
    # Execute the SQL statements in the file
    # curs_obj = cur.executescript(sql)
    # if curs_obj:
        
   
 
    try:
        cursor_obj = cur.executescript(sql_script)
         # Commit the changes and close the connection
        conn.commit()
        conn.close()
        print('Script executed successfully')
        
    except sqlite3.Error as e:
        print(f'Script failed to execute: {e}')
    


# In[39]:


creat_poc_tables('./poc_tables.sql')


# In[41]:


conn = sqlite3.connect('highend_bar.db')

table_name = 'london_daily_glass_stock'
query = f"SELECT * FROM {table_name}"
df = pd.read_sql_query(query, conn)

conn.close()


# In[43]:


printnow('END SCRIPT')


# ---
