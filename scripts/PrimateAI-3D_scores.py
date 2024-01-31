import argparse
from argparse import RawTextHelpFormatter
import pyspark
import glow
import sys

parser = argparse.ArgumentParser(
    description='Script of gene based variant filtering. \n\
    MUST BE RUN WITH spark-submit. For example: \n\
    spark-submit --driver-memory 10G Gene_based_variant_filtering.py',
    formatter_class=RawTextHelpFormatter)

parser = argparse.ArgumentParser()
parser.add_argument('-I', '--input_file', required=True,
                    help='a text file that to be converted to a partquet file')
parser.add_argument('-O', '--output_name', required=True,
                    help='name of the output parquert file')
args = parser.parse_args()

# Create spark session
spark = (
    pyspark.sql.SparkSession.builder.appName("DB2parquet")
    .getOrCreate()
    )
# Register so that glow functions like read vcf work with spark. Must be run in spark shell or in context described in help
spark = glow.register(spark)

# parameter configuration
input_file = args.input_file
output_name = args.output_name

# main 
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import DoubleType
spark.read.options(inferSchema=True,sep=",",header=True,nullValue=".") \
    .csv(input_file) \
    .withColumnRenamed("chr", "chromosome") \
    .withColumnRenamed("pos", "start") \
    .withColumnRenamed("non_flipped_ref", "reference") \
    .withColumnRenamed("non_flipped_alt", "alternate") \
    .withColumn('chromosome', regexp_replace('chromosome', 'chr', '')) \
    .withColumnRenamed("gene_name", "ensembl_transcript_ID") \
    .withColumn("score_PAI3D", col("score_PAI3D").cast(DoubleType())) \
    .coalesce(1) \
    .write.mode("overwrite") \
    .parquet(output_name)
