# Create Spark Session
import pyspark.sql
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("testApp2").getOrCreate()
spark

# Create Spark Context
from pyspark import SparkContext, SparkConf

# Create Spark Session
from pyspark.sql import SparkSession

# Create Row
from pyspark.sql import Row

# Create StructField
from pyspark.sql.types import StructField

# Create StructType
from pyspark.sql.types import StructType

# Create StringType
from pyspark.sql.types import StringType

# Create IntegerType
from pyspark.sql.types import IntegerType

# Import functions
import pyspark.sql.functions as f

# Create schema for u.data
schema = StructType([     StructField("user_id", StringType(), True),     StructField("item_id", StringType(), True),     StructField("rating", StringType(), True),     StructField("timestamp", StringType(), True)])

# Read u.data
df = spark.read.load("u.data",
                     format="csv", sep="\t", schema=schema, header="false")

# Create schema for u.item
schema = StructType([     StructField("movie_id", StringType(), True),     StructField("movie_title", StringType(), True),     StructField("release_date", StringType(), True),     StructField("video_release_date", StringType(), True),     StructField("IMDb_URL", StringType(), True),     StructField("unknown", IntegerType(), True),     StructField("Action", IntegerType(), True),     StructField("Adventure", IntegerType(), True),     StructField("Animation", IntegerType(), True),     StructField("Children's", IntegerType(), True),     StructField("Comedy", IntegerType(), True),     StructField("Crime", IntegerType(), True),     StructField("Documentary", IntegerType(), True),     StructField("Drama", IntegerType(), True),     StructField("Fantasy", IntegerType(), True),     StructField("Film-Noir", IntegerType(), True),     StructField("Horror", IntegerType(), True),     StructField("Musical", IntegerType(), True),     StructField("Mystery", IntegerType(), True),     StructField("Romance", IntegerType(), True),     StructField("Sci-Fi", IntegerType(), True),     StructField("Thriller", IntegerType(), True),     StructField("War", IntegerType(), True),     StructField("Western", IntegerType(), True)])

# Read u.item
df2 = spark.read.load("u.item",
                     format="csv", sep="|", schema=schema, header="false")

# Join dataframes
df3 = df2.join(df,df2["movie_id"] == df["item_id"]).drop(df.item_id)

# Get movie title
t0=df3.filter((f.col('movie_id') == 32) & (f.col('rating') == 2)).first()[1]

# Get count of ratings
t1=df3.filter('rating=1').filter('movie_id=32').count()
t2=df3.filter('rating=2').filter('movie_id=32').count()
t3=df3.filter('rating=3').filter('movie_id=32').count()
t4=df3.filter('rating=4').filter('movie_id=32').count()
t5=df3.filter('rating=5').filter('movie_id=32').count()
h1=df3.filter('rating=1').count()
h2=df3.filter('rating=2').count()
h3=df3.filter('rating=3').count()
h4=df3.filter('rating=4').count()
h5=df3.filter('rating=5').count()

# Create data
data = [(t1,h1),
        (t2,h2),
        (t3,h3),
        (t4,h4),
        (t5,h5)
       ]

# Create schema
schema = StructType([     StructField(t0,StringType(),True),     StructField("hist_all",StringType(),True) 
  ])

# Create dataframe
df4 = spark.createDataFrame(data=data,schema=schema)

# Print schema
df4.printSchema()

# Show dataframe
df4.show()

# Export dataframe to json
df4.toPandas().to_json('file_name_columns.json', orient='columns')
