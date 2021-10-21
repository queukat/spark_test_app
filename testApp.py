#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('pip install pyspark')
get_ipython().system('pip install pandas')
get_ipython().system('pip install pyArrow')

import pyspark.sql
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("testApp").getOrCreate()
spark


# In[2]:


#Прочитать csv файл: book.csv
#Вывести схему для dataframe полученного из п.1
#Вывести количество записей
import pyspark.pandas as ps
df = spark.read.csv('books.csv', header=True, inferSchema=True)
df.printSchema()
df.count()


# In[3]:


#Вывести информацию по книгам у которых рейтинг выше 4.50
df.filter('average_rating>4.5').show()


# In[4]:


#Вывести средний рейтинг для всех книг.
from pyspark.sql.functions import mean
df.select(mean('average_rating')).show()


# In[5]:


#Вывести агрегированную инфорацию по количеству книг в диапазонах:
#0 - 1
df.filter(df.average_rating.between (0,1.00)).count()


# In[ ]:


#Вывести агрегированную инфорацию по количеству книг в диапазонах:
#1 - 2
df.filter(df.average_rating.between (1.00,2.00)).count()


# In[ ]:


#Вывести агрегированную инфорацию по количеству книг в диапазонах:
#2 - 3
df.filter(df.average_rating.between (2.00,3.00)).count()


# In[ ]:


#Вывести агрегированную инфорацию по количеству книг в диапазонах:
#3 - 4
df.filter(df.average_rating.between (3.00,4.00)).count()


# In[ ]:


#Вывести агрегированную инфорацию по количеству книг в диапазонах:
#4 - 5
df.filter(df.average_rating.between (4.00,5.00)).count()

