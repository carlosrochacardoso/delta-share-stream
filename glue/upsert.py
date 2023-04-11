import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from delta import *

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Set paths
bucket_name = 'dataplatform-quality-gate-test'
bucket_uri = f's3://{bucket_name}'
raw_update_data = f'{bucket_uri}/raw/fruits_update.csv'
origin_table_path = f'{bucket_uri}/origin/fruit_table_origin'

# Read update batch
df_update_batch = spark.read.option("header","true").csv(raw_update_data)

# Load delta table
delta_table = DeltaTable.forPath(spark, origin_table_path)

# Upsert batch to delta table
upsert_delta_table = delta_table \
    .alias("delta_table") \
    .merge(
        df_update_batch.alias("df_update_batch"),
        "df_update_batch.id = delta_table.id"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

job.commit()