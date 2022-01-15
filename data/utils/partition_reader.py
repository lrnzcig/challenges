import json
import boto3
import botocore
from typing import Iterable

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


def get_s3_client(endpoint_url: str = None) -> botocore.client.BaseClient:
    """
    creates s3 client

    :param endpoint_url: localhost if moto_server test
    :return:
    """
    return boto3.client('s3', endpoint_url=endpoint_url)


def get_s3_file_list(
    s3_client: botocore.client.BaseClient, bucket_name: str, file_prefix: str
) -> Iterable[str]:
    """
    Gets a list of all files available in S3 for `file_prefix`

    :param s3_client:
    :param bucket_name:
    :param file_prefix:
    :return: a list of file keys
    """
    paginator = s3_client.get_paginator('list_objects')
    page_iterator = paginator.paginate(
        Bucket=bucket_name, Prefix=file_prefix
    )
    return [c['Key'] for page in page_iterator for c in page['Contents'] if 'Contents' in page.keys()]


def read_s3_file_list(
    s3_client: botocore.client.BaseClient, bucket_name: str, file_prefix_list: Iterable
) -> Iterable[dict]:
    """
    Reads all files in `file_prefix_list` and converts it to a list of jsons where:
    - the original json is present
    - partitioned datehour is added as a field
    - the token str is converted to a json and its keys are added

    :param s3_client:
    :param bucket_name:
    :param file_prefix_list:
    :return:
    """
    output = []
    for filename_key in file_prefix_list:
        # read file and convert it to list of jsons
        response = s3_client.get_object(Bucket=bucket_name, Key=filename_key)
        datehour = filename_key.split("/")[-2]
        output.extend([{
            **json.loads(el),
            **{"datehour": datehour},
            **{"token": json.loads(json.loads(el).get("user", {}).get("token"))}
        } for el in response['Body'].read().decode('utf-8').splitlines()])
    return output


def _read_files_worker(file_prefix_list: Iterable, bucket_name: str, endpoint_url: str) -> Iterable[dict]:
    """
    Read files from a prefix (which should correspond to a datehour partition) in the workers

    :param file_prefix_list:
    :param bucket_name:
    :param endpoint_url:
    :return:
    """
    s3_client = get_s3_client(endpoint_url)
    output = []
    for el in file_prefix_list:
        file_list = get_s3_file_list(s3_client, bucket_name, el)
        output.extend(read_s3_file_list(s3_client, bucket_name, file_list))
    return output


def read_in_partitions(
    spark: SparkSession, file_prefixes: Iterable[str], bucket_name: str, endpoint_url: str
) -> DataFrame:
    """
    From the list of file_prefixes, which correspond to datehour partitions, reads the files in parallel
    in the workers and generates a spark dataframe with them

    :param spark:
    :param file_prefixes:
    :param bucket_name:
    :param endpoint_url:
    :return:
    """
    rdd = spark.sparkContext.parallelize(file_prefixes, 2)
    rdd = rdd.mapPartitions(
        lambda key: _read_files_worker(key, bucket_name, endpoint_url)
    )
    return rdd.toDF()
