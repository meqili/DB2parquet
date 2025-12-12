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
                    help='a directory that contains text files to be converted to parquet files')
parser.add_argument('-O', '--output_name', required=True,
                    help='name of the output parquet file')
args = parser.parse_args()

# Create spark session
spark = (
    pyspark.sql.SparkSession.builder.appName("DB2parquet")
    .getOrCreate()
)
# Register glow
spark = glow.register(spark)

# Parameter configuration
input_database = args.input_file if args.input_file else args.input_dir
output_name = args.output_name

# Main
from pyspark.sql.functions import col, regexp_replace, regexp_extract, input_file_name
from pyspark.sql.types import LongType
allowed_chromosomes = [str(i) for i in range(1, 23)] + ["X", "Y"]
df = spark.read \
    .options(inferSchema=True, sep=",", header=True, nullValue="", nanValue="NA") \
    .csv(input_database + "/*.csv") \
    .withColumn("source_file", input_file_name()) \
    .withColumn("gene_name", regexp_extract(col("source_file"), r"variants_([^/]+)\.csv$", 1)) \
    .withColumn('chromosome', regexp_replace("Chrom", 'chr', '')) \
    .filter(col("chromosome").isin(allowed_chromosomes)) \
    .withColumn("start", col("Position").cast(LongType())) \
    .withColumn("reference", col("Ref").cast("string")) \
    .withColumn("alternate", col("Alt").cast("string")) \
    .drop('Chrom', 'Position', 'Ref', 'Alt') 

new_columns = [col.replace(" ", "_").replace(",", "_").replace(";", "_").replace("=", "_") 
                   for col in df.columns]
df = df.toDF(*new_columns)
df.coalesce(1) \
    .write.mode("overwrite") \
    .parquet(output_name)