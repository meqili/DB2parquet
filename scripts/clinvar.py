import argparse
from argparse import RawTextHelpFormatter
import pyspark
import glow
from pyspark.sql.functions import col, split
from pyspark.sql.types import LongType, StringType
import re

# Argument parser setup
parser = argparse.ArgumentParser(
    description='Script for gene-based variant filtering. \n\
    MUST BE RUN WITH spark-submit. For example: \n\
    spark-submit --driver-memory 10G Gene_based_variant_filtering.py',
    formatter_class=RawTextHelpFormatter)

parser.add_argument('-I', '--input_file', required=False,
                    help='A text file to be converted to a parquet file')
parser.add_argument('-D', '--input_dir', required=False,
                    help='Directory containing files')
parser.add_argument('-O', '--output_name', required=True,
                    help='Name of the output parquet file')
args = parser.parse_args()

# Create spark session
spark = (
    pyspark.sql.SparkSession.builder.appName("DB2parquet")
    .config('spark.sql.parquet.enableVectorizedReader', 'false')
    .getOrCreate()
)

# Register glow functions
spark = glow.register(spark)

# Parameter configuration
input_database = args.input_file if args.input_file else args.input_dir
dir_path = args.output_name

# Define allowed chromosomes
allowed_chromosomes = [str(i) for i in range(1, 23)] + ["X", "Y"]

# main
spark.read.options(inferSchema=True,sep="\t",header=True,nullValue="") \
    .csv(input_database) \
    .filter(col("chromosome").isin(allowed_chromosomes)) \
    .withColumn("start", col("start").cast(LongType())) \
    .withColumn("reference", col("reference").cast(StringType())) \
    .withColumn("alternate", col("alternate").cast(StringType())) \
    .withColumnRenamed("CLINVAR_CLNDN", "conditions") \
    .withColumnRenamed("ClinVar_Variation_ID", "VariationID") \
    .withColumnRenamed("CLINVAR_GENEINFO", "geneinfo") \
    .withColumn("clin_sig", split("CLINVAR_CLNSIG", "\|")) \
    .select('chromosome', 'start', 'reference', 'alternate', 'VariationID', 'clin_sig', 'conditions', 'geneinfo') \
    .coalesce(1) \
    .write.mode("overwrite") \
    .parquet(dir_path)