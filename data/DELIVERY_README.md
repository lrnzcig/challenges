# README for the challenge deliverable

This file explains briefly the main components of the deliverable.

## Glue job

[This job](jobs/challenge_main.py) would run AWS deployed as a Glue version 2.

Input parameters:
- bucket name
- list of partitions to be read

Steps:
1. read the partitions in parallel in the workers using the function `read_in_partitions` in [utils](utils/partition_reader.py)
2. calculates the metrics using the function `metrics_calculation` in [utils](utils/metrics_calculation.py)
3. writes the results into a parquet file

The job is untested due to time constraints, however the functions for steps 1. and 2. are tested through local tests, 
see below.

## Testing util functions

### Pre-requisites

Use python 3.7 & pyspark 2.4.3 to emulate a Glue 2.0 environment.

Create a virtual/conda environment and run:
```
pip install - r requirements.txt
```

To mock S3 services for tests, run `moto_server` from a terminal (see also TODOs [below](#test-setup)).

### Run tests

From the `data` directory, run
```
pytest tests
```

## TODOs

### Performance concerns & improvements

#### Reading files in partitions

The implemented solution (see `read_in_partitions` in [utils](utils/partition_reader.py)) 
reads the files for the different `datehour` partitions in parallel in the workers. 
While reading the files, it does some preformatting of the data (basically it converts the token string field to a json)
with the advantage of doing only one pass through the data. However it uses a python function, which is not native to 
Spark, and will have a performance penalty.

Alternatives would be:
1. rewriting the code in [partition reader](utils/partition_reader.py) to Java or Scala
2. reading the files from json in python in a 1st pass, and then using `from_json` function in Spark to convert the `token` column

Personally I would be inclined to think that 1. would work better, but only a performance test could tell.

#### Metrics calculation

The implemented solution (see `metrics_calculation` in [utils](utils/metrics_calculation.py))
is a first straight forward version that works functionally but can be certainly tuned.

Improvements:
1. `count_type_all` and `count_type_w_consent` can be solved in one group by that pivots by `type` and `consent`; this would
give counts of events with and without consent, and to get the totals both should be added in the grouped df
2. `avg_pageviews_p_user` could be better calculated with a Window function

### Test setup
Improvements:
- refactor tests to run `moto_server` from a fixture in the tests themselves, not needing an external script
- use a base test class that starts and stops spark and combines the test utility methods now splitted in the 2 tests
- after the above is done, improve test coverage (see TODOs) in the code itself

### Final note
Refactoring is an infinite task!
