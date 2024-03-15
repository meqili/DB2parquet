import argparse
from argparse import RawTextHelpFormatter
import pyspark
import glow

parser = argparse.ArgumentParser(
    description='Script of gene based variant filtering. \n\
    MUST BE RUN WITH spark-submit. For example: \n\
    spark-submit --driver-memory 10G Gene_based_variant_filtering.py',
    formatter_class=RawTextHelpFormatter)

parser = argparse.ArgumentParser()
parser.add_argument('-I', '--input_file', required=False,
                    help='a text file that to be converted to a partquet file')
parser.add_argument('-D', '--input_dir', required=False,
                    help='Directory containing files')
parser.add_argument('-O', '--output_name', required=True,
                    help='name of the output parquert file')
args = parser.parse_args()

# Create spark session
spark = (
    pyspark.sql.SparkSession.builder.appName("DB2parquet")
    .config('spark.sql.parquet.enableVectorizedReader', 'false')
    .getOrCreate()
    )
# Register so that glow functions like read vcf work with spark. Must be run in spark shell or in context described in help
spark = glow.register(spark)

# parameter configuration
if args.input_file:
    input_database = args.input_file
else:
    input_database = args.input_dir
dir_path = args.output_name


## mian 
from pyspark.sql import functions as f
from pyspark.sql.types import LongType
import re

dbNSFP_variant = spark.read.options(inferSchema=True,sep="\t",header=True,nullValue=".") \
            .csv(input_database)

cols=[re.sub(r'(^_|_$)','',f.replace("(", "[").replace(" ", "_").replace(")", "]")) for f in dbNSFP_variant.columns]
dbNSFP_variant.toDF(*cols) \
    .withColumnRenamed("#chr", "chromosome") \
    .withColumn("pos[1-based]", f.col("pos[1-based]").cast(LongType())) \
    .withColumnRenamed("pos[1-based]", "start") \
    .withColumnRenamed("ref", "reference") \
    .withColumnRenamed("alt", "alternate") \
    .write.mode("overwrite") \
    .partitionBy("chromosome") \
    .parquet(dir_path)
