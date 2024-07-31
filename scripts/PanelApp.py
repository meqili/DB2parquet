import argparse
from argparse import RawTextHelpFormatter
import pyspark
import glow
import os

parser = argparse.ArgumentParser(
    description='Script of gene based variant filtering. \n\
    MUST BE RUN WITH spark-submit. For example: \n\
    spark-submit --driver-memory 10G Gene_based_variant_filtering.py',
    formatter_class=RawTextHelpFormatter)

parser = argparse.ArgumentParser()
parser.add_argument('-I', '--input_file', required=False,
                    help='a text file that to be converted to a partquet file')
parser.add_argument('-D', '--input_dir', required=False,
                    help='a directory that conatins text files to be converted to partquet files')
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
input_database = args.input_file if args.input_file else args.input_dir
output_name = args.output_name

# untar tar fie
import tarfile
tar = tarfile.open(input_database)
tar.extractall()
tar.close()

# main 
from pyspark.sql.functions import col, input_file_name
from pyspark.sql.types import LongType
import re
# Read all TSV files in the directory into a DataFrame
Panel_df = spark.read.options(inferSchema=True,sep="\t",header=True,nullValue="",ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True) \
                 .csv("*.tsv") \
                 .withColumn("file_name", input_file_name()) \
                 .withColumnRenamed("Sources(; separated)", "Sources_semicolon_separated") \
                 .withColumnRenamed("Position Chromosome", "chromosome") \
                 .withColumn("start", col("Position GRCh38 Start").cast(LongType())) \
                 .withColumn("end", col("Position GRCh38 End").cast(LongType())) \
                 .drop("Sources(; separated)", "Position Chromosome", "Position GRCh38 Start", "Position GRCh38 End")

# Get a list of current column names
current_cols = Panel_df.columns

# Define a function to replace symbols with underscores
def clean_col_name(col_name):
    return re.sub('[ ,;{}()\n\t=]+', '_', col_name)

# Use list comprehension to create a list of new column names with symbols replaced by underscores
new_cols = [clean_col_name(col_name) for col_name in current_cols]

# Rename the columns using withColumnRenamed()
for i in range(len(current_cols)):
    Panel_df = Panel_df.withColumnRenamed(current_cols[i], new_cols[i])

Panel_df.coalesce(1) \
     .write.mode("overwrite") \
     .parquet(output_name)