from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName('BookAnalysis').getOrCreate()

# Read csv file
df = spark.read.csv('book.csv', header=True, inferSchema=True)

# Output the schema for the data frame
df.printSchema()

# Output the number of records
print("Number of records:", df.count())

# Output information on books with a rating higher than 4.50
df.filter(df.rating > 4.50).show()

# Output the average rating for all books
print("Average rating:", df.agg({'rating': 'avg'}).collect()[0][0])

# Output aggregated information on the number of books in the ranges: 0 - 1
print("Number of books in the range 0 - 1:", df.filter((df.rating >= 0) & (df.rating < 1)).count())

# Output aggregated information on the number of books in the ranges: 1 - 2
print("Number of books in the range 1 - 2:", df.filter((df.rating >= 1) & (df.rating < 2)).count())

# Output aggregated information on the number of books in the ranges: 2 - 3
print("Number of books in the range 2 - 3:", df.filter((df.rating >= 2) & (df.rating < 3)).count())

# Output aggregated information on the number of books in the ranges: 3 - 4
print("Number of books in the range 3 - 4:", df.filter((df.rating >= 3) & (df.rating < 4)).count())

# Output aggregated information on the number of books in the ranges: 4 - 5
print("Number of books in the range 4 - 5:", df.filter((df.rating >= 4) & (df.rating <= 5)).count())
