import argparse
from argparse import RawTextHelpFormatter
import pyspark
import glow
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import regexp_replace, col
import os
import sys

# Argument parser
parser = argparse.ArgumentParser(
    description='Script of gene based variant filtering. \n\
    MUST BE RUN WITH spark-submit. For example: \n\
    spark-submit --driver-memory 10G Gene_based_variant_filtering.py',
    formatter_class=RawTextHelpFormatter)

parser.add_argument('-I', '--input_file', required=False,
                    help='a text file that to be converted to a parquet file')
parser.add_argument('-D', '--input_dir', required=False,
                    help='a directory that contains text files to be converted to parquet files')
parser.add_argument('-O', '--output_name', required=True,
                    help='name of the output parquet file')
args = parser.parse_args()

# Create Spark session
spark = (
    pyspark.sql.SparkSession.builder.appName("DB2parquet")
    .getOrCreate()
)

# Register Glow so that VCF reading works with Spark
spark = glow.register(spark)

# Parameter configuration
input_database = args.input_file if args.input_file else args.input_dir
output_name = args.output_name

# Define schema for input data
schema = StructType([
    StructField("bin", IntegerType(), True),
    StructField("chromosome", StringType(), True),
    StructField("start", IntegerType(), True),
    StructField("end", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("strand", StringType(), True),
    StructField("thickStart", IntegerType(), True),
    StructField("thickEnd", IntegerType(), True),
    StructField("itemRgb", StringType(), True),
])

# List of input files to process
files = [
    "wgEncodeBroadHmmGm12878HMM.bed", "wgEncodeBroadHmmHepg2HMM.bed", 
    "wgEncodeBroadHmmHsmmHMM.bed", "wgEncodeBroadHmmK562HMM.bed", 
    "wgEncodeBroadHmmNhlfHMM.bed", "wgEncodeBroadHmmH1hescHMM.bed", 
    "wgEncodeBroadHmmHmecHMM.bed", "wgEncodeBroadHmmHuvecHMM.bed",
    "wgEncodeBroadHmmNhekHMM.bed"
]

# Processing each file
for FILE in files:
    try:
        file_path = os.path.join(input_database, FILE)
        
        # Ensure file exists before processing
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Input file {FILE} not found at {file_path}.")
        
        # Extract the base file name (without extension)
        base_name, _ = os.path.splitext(os.path.basename(file_path))
        
        # Read the file and process
        spark.read.options(sep="\t", header=False, nullValue=".") \
            .schema(schema) \
            .csv(file_path) \
            .withColumn('chromosome', regexp_replace('chromosome', 'chr', '')) \
            .withColumn("start", col("start") + 1) \
            .withColumn("thickStart", col("thickStart") + 1) \
            .coalesce(1) \
            .write.mode("overwrite") \
            .parquet(output_name + '/' + base_name)
        
        print(f"Processed {FILE} successfully.")

    except Exception as e:
        # Capture any exceptions and print the error message
        print(f"Error processing {FILE}: {str(e)}", file=sys.stderr)
