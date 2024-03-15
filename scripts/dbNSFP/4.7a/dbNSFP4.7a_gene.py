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

# Define the path to the directory containing the files
dir_path = args.output_name

# Read all TSV files in the directory into a DataFrame
dbNSFP_gene = spark.read.options(inferSchema=True,sep="\t",header=True, nullValue=".") \
    .csv(args.input_file) \
    .withColumnRenamed("chr", "chromosome")

new_columns = [col.replace('(', '[').replace(')', ']').replace(' ', '_') for col in dbNSFP_gene.columns]
dbNSFP_gene = dbNSFP_gene.toDF(*new_columns)

    
dbNSFP_gene.coalesce(1) \
        .write.mode("overwrite") \
        .parquet(dir_path)
