from pyspark.sql import SparkSession
from pyspark.sql.functions import col

ipath='s3://dep-bucket-dp/Source-data/Stock_ds_data.txt'
opath='s3://dep-bucket-dp/Output-data/stockout'
sp=SparkSession.builder.appName('Stocks').getOrCreate()
df=sp.read.csv(ipath,inferSchema=True,header=True)
df=df.filter(col('Stock Name').startswith('A'))
df.write.parquet(opath,mode='overwrite')