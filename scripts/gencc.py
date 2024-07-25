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
inputs = args.input_file
output_name = args.output_name

# main 
import csv

output_file = output_name + ".tsv"

with open(inputs, 'r', newline='', encoding='utf-8') as infile, \
     open(output_file, 'w', newline='', encoding='utf-8') as outfile:
    reader = csv.reader(infile, delimiter='\t', quotechar='"', quoting=csv.QUOTE_ALL)
    writer = csv.writer(outfile, delimiter='\t', quotechar='"', quoting=csv.QUOTE_ALL)
    for row in reader:
        cleaned_row = [field.replace('\n', ' ') for field in row]
        writer.writerow(cleaned_row)

spark.read.options(inferSchema=True,sep="\t",header=True,nullValue="") \
    .csv(output_file) \
    .coalesce(1) \
    .write.mode("overwrite") \
    .parquet(output_name) 