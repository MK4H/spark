#!/usr/bin/env python3

import sys

from pyspark.sql import SparkSession, SQLContext, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType



def main():
    if len(sys.argv) != 2:
        print(f"Invalid number of arguments, expected 2, found {len(sys.argv)}", file=sys.stderr)
        sys.exit(1)

    inputPath = sys.argv[1]

    session = SparkSession.builder.appName("TelSearch").getOrCreate()

    schema = StructType([
        StructField("first_name", StringType(), nullable=False),
        StructField("last_name", StringType(), nullable=False),
        StructField("tel_num", StringType(), nullable=False),
        StructField("psc", IntegerType(), nullable=False)
    ])

    df: DataFrame = session.read.csv(inputPath, header=False, schema=schema)
    df.withColumn("region", df["psc"] / 10000) \
        .groupBy(["region", "first_name", "last_name"]) \
        .count() \
        .filter("count" >= 2 ) \
        .show()

if __name__ == "__main__":
    main()