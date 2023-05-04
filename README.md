# QS Analytics Engineering Exercise


## Overview

The aim of this project is to create a database from the bar data provided by the client along with supporting data from an online cocktail database found at (https://www.thecocktaildb.com/api.php).

The project consists of a python script which loads, processes and uploads the data to the sqlite database using the accompanying .sql files found in this project. The database has been named 'highend_bar.db'.

'data_tables.sql' contains the queries used to create and populate the database tables.

'poc_tables.sql' contains the queries used to create the proof-of-concept tables the client can use to get his data journey underway. 


## Requirements
You will need sqlite, Python, pandas, json, requests, glob, re and time to run the script

## Assumptions
As the bars are 24 hr and have consisent hourly traffic I have grouped the data as daily to reduce the overhead in data storage. On each day the data is grouped by date, bar_no (created variable to identify bars) and drink with the total amount transacted summed and a count for the number of drinks sold.

While the focus is on the glass stocks at each bar I have also included the amount of money transacted for each drink to provide further context on the glass shortage problem.

## Challenges
This was my first time using github so I do not expect my practices to be optimal or best standard.
