import os
import unittest

from pyspark.sql import SparkSession
from pyspark.context import SparkContext, SparkConf

from utils.partition_reader import get_s3_client, get_s3_file_list, read_s3_file_list, read_in_partitions

BUCKET_NAME = 'didomi_test'
BUCKET_PREFIX = 'input_data'

os.environ["AWS_ACCESS_KEY_ID"] = "mock"
os.environ["AWS_SECRET_ACCESS_KEY"] = "mock"
# os.environ["TEST_SERVER_MODE"] = "True"  TODO investigate this


class TestPartitionReader(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # create spark instance with S3
        spark_conf = SparkConf().setAppName("test")
        spark_conf.set('spark.jars.packages', ','.join([
            'org.apache.hadoop:hadoop-aws:2.7.3',
        ]))

        spark_context = SparkContext(conf=spark_conf).getOrCreate()
        spark_context._jsc.hadoopConfiguration().set('fs.s3.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        spark_context._jsc.hadoopConfiguration().set(
            'fs.s3a.access.key',
            'fake'
        )
        spark_context._jsc.hadoopConfiguration().set(
            'fs.s3a.secret.key',
            'fake'
        )
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://127.0.0.1:5000")
        cls.spark = SparkSession.builder.appName('test').getOrCreate()

        # input files and upload to mocked bucket
        cls.input_files = [
            "../data/input/datehour=2021-01-23-10/part-00000-9b503de5-0d72-4099-a388-7fdcf6e16edb.c000.json",
            "../data/input/datehour=2021-01-23-11/part-00000-3a6f210d-7806-462e-b32c-344dff538d70.c000.json"
        ]

        cls.s3_client = get_s3_client("http://127.0.0.1:5000")
        cls.s3_client.create_bucket(Bucket=BUCKET_NAME)

        for file_name in cls.input_files:
            with open(file_name, 'r') as f:
                cls.s3_client.put_object(
                    Bucket=BUCKET_NAME,
                    Key='/'.join([BUCKET_PREFIX, file_name.split('/')[-2], file_name.split('/')[-1]]),
                    Body=f.read()
                )

        # some expected values for convinience
        cls.input_files_bucket = ['/'.join([BUCKET_PREFIX] + el.split('/')[-2:]) for el in cls.input_files]
        cls.expected_output_columns = sorted(['id', 'datetime', 'domain', 'type', 'user', 'datehour', 'token'])

    def test_get_s3_file_list(self):
        file_list = get_s3_file_list(self.s3_client, BUCKET_NAME, BUCKET_PREFIX)
        assert sorted(file_list) == sorted(self.input_files_bucket)

    def test_read_s3_file_list(self):
        file_name = self.input_files_bucket[0]
        json_list = read_s3_file_list(self.s3_client, BUCKET_NAME, [file_name])
        assert len(json_list) == 39
        assert sorted(list(json_list[0].keys())) == self.expected_output_columns
        # TODO add assert for file content

    def test_read_in_partitions(self):
        file_prefixes = set(["/".join(f.split("/")[:-1]) for f in self.input_files_bucket])
        df = read_in_partitions(self.spark, file_prefixes, BUCKET_NAME, "http://127.0.0.1:5000")
        assert sorted(df.columns) == self.expected_output_columns
        assert df.count() == 62
        # TODO assert for df content
