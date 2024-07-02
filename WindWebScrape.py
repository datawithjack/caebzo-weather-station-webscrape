#!/usr/bin/env python
# coding: utf-8

# ## WindWebScrape
# 
# New notebook

# In[37]:


# Welcome to your new notebook
# Type here in the cell editor to add code!
# import packages 
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import datetime
from datetime import date


URL = 'https://cabezo.bergfex.at/'
response = requests.get(URL)
soup = BeautifulSoup(response.content, 'lxml')

# CSS selectors based on the provided XPaths
selectors = {
    'WindSpeedTime': 'body > header > v:nth-child(9) > span',
    'WindSpeed': 'html > body > header > v:nth-of-type(1) > span',
    'WindGust': 'body > header > v:nth-child(5)',
    'Temperature': 'body > header > v.small.mobile-hidden'
}

data = {}
for key, selector in selectors.items():
    element = soup.select_one(selector)
    data[key] = element.text.strip() if element else 'Not Found'

# Convert the dictionary to a DataFrame
df = pd.DataFrame([data])
print(df) # check output

df['WindSpeed'] = df['WindSpeed'].str.replace('[a-zA-Z]', '').astype(float)
df['WindGust'] = df['WindGust'].str.replace('[a-zA-Z]', '').astype(float)
df['Temperature'] = df['Temperature'].str.replace('°C', '').astype(float)
print(df)


# In[38]:


from pyspark.sql import Row
sdf = spark.createDataFrame(df)


# In[39]:


from pyspark.sql.types import IntegerType
from pyspark.sql.types import TimestampType
from pyspark.sql.types import FloatType

sdf = sdf.withColumn("WindSpeed",sdf.WindSpeed.cast(FloatType()))
sdf = sdf.withColumn("WindGust",sdf.WindGust.cast(FloatType()))
sdf = sdf.withColumn("WindSpeedTime",sdf.WindSpeedTime.cast(TimestampType()))
sdf = sdf.withColumn("Temperature",sdf.Temperature.cast(FloatType()))
sdf.printSchema()


# In[40]:


sdf.show()


# In[41]:


delta_table_path = "Tables/CabezoWind"
sdf.write.format("delta").mode("append").save(delta_table_path)

