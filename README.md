# Spark

This repository contains my solution to the Apache Spark assignment.
The solution contains following files:

- spark-slurm.sh
- telsearch/telsearch.py

## spark-slurm.sh

This file is passed to the sbatch command to start the computation.
It is started without any arguments as everything is hardcoded inside for
easier debugging and testing.

As you can see from the file, the job is started with the following settings:
- 6 nodes (as w201 and w202 were unavailable during testing)
- 1 task (executor in the case of spark) per node
- full 64 CPUs per executor
- 96G of RAM per task (128GB refused to start, and 96GB was closest roundest number :D)

Each executor is started with 64 cores, but only with 64GB of memory (as it only specifies some memory used by the executor). Driver is given 32G of memory, just to hopefully fit on the same node as one of the executors.

These executor and driver settings are passed to spark-submit, as they were not working in the spark-defaults.sh (don't know why).

All fixes from the Slack conversation are of course applied to the submit script.

## telsearch/telsearch.py

This file contains the program run by Spark. It expects the input file path and output directory path as first and second argument.

We then use the pyspark.sql.DataFrame API to process the data.

The data is first partitioned by `first_name` and `last_name`, to match the later groupBy. Wihtout this prepartitioning, the solution runs about 10% slower.

Then we simply add the region column by taking the first number of the psc column,
group everything by `first_name`, `last_name` and `region`, which gives us groups of
people with exactly the same name in the same region, then to each of these groups
we add `count` column with the number of entries in each group and finally we filter out groups with only a single entry.

Unfortunately the Python version available to Spark in the cluster is older than 3.6, so there are no type hints or fstring interpolation.

# Results

Running on 6 nodes (as w201 and w202 were unavailable at the time of testing) using the settings
in the spark-slurm.sh file, I got the runtime to about 3.43 minutes. The result is spread out among many files to make the programming easier, but even without concatenating it, the result is useful. It can for example be searched using grep.
