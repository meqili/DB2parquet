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
from pyspark.sql.functions import col, when
from pyspark.sql.types import LongType

# Read the CSV file
df = spark.read.options(inferSchema=True, sep="\t", header=True).csv(input_database)
new_columns = [col.replace('.', '_') for col in df.columns]
df = df.toDF(*new_columns)

# Replace '-' with null for all columns except 'orientation'
for column in df.columns:
    if column != 'orientation':
        df = df.withColumn(column, when(col(column) == '-', None).otherwise(col(column)))

# Cast columns to appropriate types and drop unnecessary ones
df = df.withColumn("start", col("start_position_on_the_genomic_accession").cast(LongType())) \
       .withColumn("end", col("end_position_on_the_genomic_accession").cast(LongType())) \
       .drop("start_position_on_the_genomic_accession", "end_position_on_the_genomic_accession")

# Write the resulting DataFrame to Parquet format
df.coalesce(1).write.mode("overwrite").parquet(output_name)