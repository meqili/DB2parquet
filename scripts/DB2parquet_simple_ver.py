import argparse
from argparse import RawTextHelpFormatter
import pyspark
import glow

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
    pyspark.sql.SparkSession.builder.appName("DB2parquet_ez")
    .getOrCreate()
)
# Register Glow
spark = glow.register(spark)

# Select input path
input_database = args.input_file if args.input_file else args.input_dir
output_name = args.output_name

# Main logic: read text file and write as Parquet
spark.read.options(inferSchema=True, sep=sep, header=True, nullValue="") \
    .csv(input_database) \
    .coalesce(1) \
    .write.mode("overwrite") \
    .parquet(output_name)
