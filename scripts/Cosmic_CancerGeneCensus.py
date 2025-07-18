import argparse
from argparse import RawTextHelpFormatter
import pyspark
import glow
import re
from pyspark.sql.functions import when, col

# Define argument parser
parser = argparse.ArgumentParser(
    description='This version of DB2parquet keeps the original formats of databases',
    formatter_class=RawTextHelpFormatter)

parser.add_argument('-I', '--input_file', required=False,
                    help='A text file to be converted to a Parquet file')
parser.add_argument('-D', '--input_dir', required=False,
                    help='A directory containing text files to be converted to Parquet files')
parser.add_argument('-O', '--output_name', required=True,
                    help='Name of the output Parquet file')
parser.add_argument('-S', '--field_separator', required=False, default='comma',
                    help='Field separator: options are "comma", "tab", or provide a single character like ";" or "|"')

args = parser.parse_args()

# Determine actual separator character
separator_map = {
    'comma': ',',
    'tab': '\t',
    'semicolon': ';',
    'pipe': '|'
}
sep = separator_map.get(args.field_separator.lower(), args.field_separator)

# Create Spark session
spark = (
    pyspark.sql.SparkSession.builder.appName("DB2parquet")
    .getOrCreate()
)
# Register Glow
spark = glow.register(spark)

# Select input path
input_database = args.input_file if args.input_file else args.input_dir
output_name = args.output_name

cgc_df = spark.read.options(inferSchema=True, sep=sep, header=True, nullValue="") \
    .csv(input_database) 
cgc_df = cgc_df.withColumn(
    "Entrez GeneId",
    when((col("Gene Symbol") == "HMGN2P46") & col("Entrez GeneId").isNull(), 283651)
    .when((col("Gene Symbol") == "MALAT1") & col("Entrez GeneId").isNull(), 378938)
    .when((col("Gene Symbol") == "MDS2") & col("Entrez GeneId").isNull(), 259283)
    .otherwise(col("Entrez GeneId"))
)

# Get a list of current column names
current_cols = cgc_df.columns

# Define a function to replace symbols with underscores
def clean_col_name(col_name):
    return re.sub('[ ,;{}()\n\t=]+', '_', col_name)

# Use list comprehension to create a list of new column names with symbols replaced by underscores
new_cols = [clean_col_name(col_name) for col_name in current_cols]

# Rename the columns using withColumnRenamed()
for i in range(len(current_cols)):
    cgc_df = cgc_df.withColumnRenamed(current_cols[i], new_cols[i])

cgc_df.coalesce(1) \
     .write.mode("overwrite") \
     .parquet(output_name)