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
parser.add_argument('-I', '--input_file', required=True,
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

# main 
spark.read.options(inferSchema=True,sep=",",header=True,nullValue=".") \
    .csv(input_database) \
    .coalesce(1) \
    .write.mode("overwrite") \
    .parquet(output_name)
