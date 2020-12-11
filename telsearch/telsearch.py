import sys

from pyspark.sql import SparkSession, SQLContext, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
import pyspark.sql.functions as fn


def main():
    print(sys.version)
    if len(sys.argv) != 3:
        # Pre 3.6 python version, does not support fstrings
        # print(f"Invalid number of arguments, expected 2, found {len(sys.argv)}", file=sys.stderr)
        sys.exit(1)

    inputPath = sys.argv[1]
    outputPath = sys.argv[2]

    session = SparkSession.builder.appName("TelSearch").getOrCreate()

    schema = StructType([
        StructField("first_name", StringType(), nullable=False),
        StructField("last_name", StringType(), nullable=False),
        StructField("tel_num", StringType(), nullable=False),
        StructField("psc", IntegerType(), nullable=False)
    ])
    # Old python version, does not support type hinting
    #df: DataFrame = session.read.csv(inputPath, header=False, schema=schema)
    df = session.read.csv(inputPath, header=False, schema=schema)

    df = df.repartition("first_name", "last_name")
    df.withColumn("region", df["psc"].cast(StringType()).substr(0, 1)) \
        .groupBy("first_name", "last_name", "region") \
        .count() \
        .filter(fn.column("count") > 1) \
        .write.csv(outputPath)


if __name__ == "__main__":
    main()