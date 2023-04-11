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
logger = glueContext.get_logger()

#Set paths
bucket_name = 'dataplatform-quality-gate-test'
bucket_uri = f's3://{bucket_name}'
origin_delta_share_path = f'{bucket_uri}/origin/profile/config.share#schema1.fruits.origin_fruit_table'
target_table_path = f'{bucket_uri}/target/fruit_table_target'
target_checkpoint_path = f'{bucket_uri}/target/fruit_table_target/_checkpoints'

# Load target table
df_target = DeltaTable.forPath(spark, target_table_path)

# Function to upsert df_micro_batch into Delta table using merge
def upsert_to_delta(df_micro_batch, batch_id):
    df_micro_batch = df_micro_batch.where("_change_type <> 'update_preimage'")
    df_target.alias("df_target").merge(
            df_micro_batch.alias("micro_batch_table"),
            "micro_batch_table.id = df_target.id") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

# Load origin stream
df_origin = spark.readStream \
    .option("ignoreChanges", "true") \
    .format("deltaSharing") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .load(origin_delta_share_path)

# Write the output of a streaming aggregation query into Delta table
query = df_origin.writeStream \
  .format("delta") \
  .foreachBatch(upsert_to_delta) \
  .outputMode("update") \
  .option("checkpointLocation", target_checkpoint_path) \
  .start()

query.awaitTermination()

job.commit()