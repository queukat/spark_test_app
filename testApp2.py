#!/usr/bin/env python
# coding: utf-8

# In[ ]:


get_ipython().system('pip install pyspark')
get_ipython().system('pip install pandas')
get_ipython().system('pip install pyArrow')

import pyspark.sql
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("testApp2").getOrCreate()
spark

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as f

schema = StructType([     StructField("user_id", StringType(), True),     StructField("item_id", StringType(), True),     StructField("rating", StringType(), True),     StructField("timestamp", StringType(), True)])
df = spark.read.load("u.data",
                     format="csv", sep="\t", schema=schema, header="false")

schema = StructType([     StructField("movie_id", StringType(), True),     StructField("movie_title", StringType(), True),     StructField("release_date", StringType(), True),     StructField("video_release_date", StringType(), True),     StructField("IMDb_URL", StringType(), True),     StructField("unknown", IntegerType(), True),     StructField("Action", IntegerType(), True),     StructField("Adventure", IntegerType(), True),     StructField("Animation", IntegerType(), True),     StructField("Children's", IntegerType(), True),     StructField("Comedy", IntegerType(), True),     StructField("Crime", IntegerType(), True),     StructField("Documentary", IntegerType(), True),     StructField("Drama", IntegerType(), True),     StructField("Fantasy", IntegerType(), True),     StructField("Film-Noir", IntegerType(), True),     StructField("Horror", IntegerType(), True),     StructField("Musical", IntegerType(), True),     StructField("Mystery", IntegerType(), True),     StructField("Romance", IntegerType(), True),     StructField("Sci-Fi", IntegerType(), True),     StructField("Thriller", IntegerType(), True),     StructField("War", IntegerType(), True),     StructField("Western", IntegerType(), True)])
df2 = spark.read.load("u.item",
                     format="csv", sep="|", schema=schema, header="false")


# In[ ]:


df3 = df2.join(df,df2["movie_id"] == df["item_id"]).drop(df.item_id)


# In[ ]:


t0=df3.filter((f.col('movie_id') == 32) & (f.col('rating') == 2)).first()[1]


# In[ ]:


t1=df3.filter('rating=1').filter('movie_id=32').count()


# In[ ]:


t2=df3.filter('rating=2').filter('movie_id=32').count()


# In[ ]:


t3=df3.filter('rating=3').filter('movie_id=32').count()


# In[ ]:


t4=df3.filter('rating=4').filter('movie_id=32').count()


# In[ ]:


t5=df3.filter('rating=5').filter('movie_id=32').count()


# In[ ]:


h1=df3.filter('rating=1').count()


# In[ ]:


h2=df3.filter('rating=2').count()


# In[ ]:


h3=df3.filter('rating=3').count()


# In[ ]:


h4=df3.filter('rating=4').count()


# In[ ]:


h5=df3.filter('rating=5').count()


# In[ ]:


data = [(t1,h1),
        (t2,h2),
        (t3,h3),
        (t4,h4),
        (t5,h5)
       ]

schema = StructType([     StructField(t0,StringType(),True),     StructField("hist_all",StringType(),True) 
  ])
 
df4 = spark.createDataFrame(data=data,schema=schema)
df4.printSchema()
df4.show()


# In[ ]:





# In[ ]:


df4.toPandas().to_json('file_name_columns.json', orient='columns')

