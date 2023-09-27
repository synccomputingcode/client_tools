# Databricks notebook source
# MAGIC %pip install openpyxl==3.1.2
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Install Required Libraries
import pandas as pd
import numpy as np
from datetime import timedelta

import pyspark.sql.functions as fn
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md ##Step 1: Access the Data
# MAGIC
# MAGIC The dataset we will use for this exercise is the [Online Retail Data Set](http://archive.ics.uci.edu/ml/datasets/Online+Retail) available from the UCI Machine Learning Repository:

# COMMAND ----------

# MAGIC %sh 
# MAGIC  
# MAGIC rm -rf /dbfs/tmp/clv/online_retail  # drop any old copies of data
# MAGIC mkdir -p /dbfs/tmp/clv/online_retail # ensure destination folder exists
# MAGIC  
# MAGIC # download data to destination folder
# MAGIC wget -N http://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx -P /dbfs/tmp/clv/online_retail
# MAGIC

# COMMAND ----------

# MAGIC %md The dataset is made available as an Excel spreadsheet.  We can read this data to a pandas dataframe as follows:

# COMMAND ----------

# DBTITLE 1,Read Data
xlsx_filename = "/dbfs/tmp/clv/online_retail/Online Retail.xlsx"
 
# schema of the excel spreadsheet data range
orders_schema = {
  'InvoiceNo':str,
  'StockCode':str,
  'Description':str,
  'Quantity':np.int64,
  'InvoiceDate':np.datetime64,
  'UnitPrice':np.float64,
  'CustomerID':str,
  'Country':str  
  }
 
# read spreadsheet to pandas dataframe
# the xlrd library must be installed for this step to work 
orders_pd = pd.read_excel(
  xlsx_filename, 
  sheet_name='Online Retail',
  header=0, # first row is header
  dtype=orders_schema
  )
 
# calculate sales amount as quantity * unit price
orders_pd['SalesAmount'] = orders_pd['Quantity'] * orders_pd['UnitPrice']
 
# display first few rows from the dataset
orders_pd.head(10)

# COMMAND ----------

# MAGIC %md The data in the workbook are organized as a range in the Online Retail spreadsheet.  Each record represents a line item in a sales transaction. The fields included in the dataset are:
# MAGIC
# MAGIC | Field | Description |
# MAGIC |-------------:|-----:|
# MAGIC |InvoiceNo|A 6-digit integral number uniquely assigned to each transaction|
# MAGIC |StockCode|A 5-digit integral number uniquely assigned to each distinct product|
# MAGIC |Description|The product (item) name|
# MAGIC |Quantity|The quantities of each product (item) per transaction|
# MAGIC |InvoiceDate|The invoice date and a time in mm/dd/yy hh:mm format|
# MAGIC |UnitPrice|The per-unit product price in pound sterling (£)|
# MAGIC |CustomerID| A 5-digit integral number uniquely assigned to each customer|
# MAGIC |Country|The name of the country where each customer resides|
# MAGIC |SalesAmount| Derived as Quantity * UnitPrice |
# MAGIC
# MAGIC Of these fields, the ones of particular interest for our work are InvoiceNo which identifies the transaction, InvoiceDate which identifies the date of that transaction, and CustomerID which uniquely identifies the customer across multiple transactions. The SalesAmount field is derived from the Quantity and UnitPrice fields in order to provide as a monetary amount around which we can estimate value.
# MAGIC
