import sys

from awsglue.transforms import *
from pyspark.context import SparkContext, SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from utils.partition_reader import get_s3_client, get_s3_file_list, read_in_partitions
from utils.metrics_calculation import metrics_calculation

conf = SparkConf()
sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
resolved_args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME', 'BUCKET_PREFIX'])
job.init(resolved_args['JOB_NAME'], resolved_args)

# processing starts
s3_client = get_s3_client()
# 0. figure out file_prefixes to be processed - should be an input parameter for the partitions to run
file_list = get_s3_file_list(s3_client, resolved_args['BUCKET_NAME'], resolved_args['BUCKET_PREFIX'])
file_prefixes = set(["/".join(f.split("/")[:-1]) for f in file_list])

# 1. read files
df = read_in_partitions(spark, file_prefixes, resolved_args['BUCKET_NAME'])

# 2. calculate the metrics
result = metrics_calculation(df)

# 3. write metric results to parquet
result.write.mode("overwrite").format("parquet").save("results." + resolved_args['JOB_NAME'] + ".parquet")
