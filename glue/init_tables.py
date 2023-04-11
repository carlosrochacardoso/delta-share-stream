import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")

# Set paths
bucket_name = '<bucket_name>'
bucket_uri = f's3://{bucket_name}'
raw_data = f'{bucket_uri}/raw/fruits.csv'
origin_table_path = f'{bucket_uri}/origin/fruit_table_origin'
target_table_path = f'{bucket_uri}/target/fruit_table_target'

# Read raw data
df_raw = spark.read.option("header","true").csv(raw_data)

# Init origin table
df_raw.write.format("delta") \
    .mode("overwrite") \
    .save(origin_table_path)

# Init target table
df_raw.write.format("delta") \
    .mode("overwrite") \
    .save(target_table_path)

job.commit()