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

# untar tar fie
import tarfile
tar = tarfile.open(input_database)
tar.extractall()
tar.close()

# main
## phenotype.hpoa
# Read the input file, skipping lines starting with #
lines = spark.sparkContext.textFile("phenotype.hpoa").filter(lambda line: not line.startswith("#"))

# Split the header line to get column names
header = lines.first()
columns = header.split("\t")

# Create a DataFrame by skipping the header and splitting on tabs
data = lines.filter(lambda line: line != header).map(lambda line: line.split("\t"))

# Convert the RDD to a DataFrame
df = spark.createDataFrame(data, columns)

# Save the DataFrame as a Parquet file
df.write.parquet(output_name + "/HPO_phenotype.hpoa")

## genes_to_disease.txt
spark.read.options(inferSchema=True,sep="\t",header=True,nullValue="") \
    .csv("genes_to_disease.txt") \
    .coalesce(1) \
    .write.mode("overwrite") \
    .parquet(output_name + "/HPO_genes_to_disease")

## genes_to_phenotype.txt
spark.read.options(inferSchema=True,sep="\t",header=True,nullValue="-") \
    .csv("genes_to_phenotype.txt") \
    .coalesce(1) \
    .write.mode("overwrite") \
    .parquet(output_name + "/HPO_genes_to_phenotype")

## phenotype_to_genes.txt
spark.read.options(inferSchema=True,sep="\t",header=True,nullValue="") \
    .csv("phenotype_to_genes.txt") \
    .coalesce(1) \
    .write.mode("overwrite") \
    .parquet(output_name + "/HPO_phenotype_to_genes")

spark.read.options(inferSchema=True,sep="\t",header=True,nullValue="") \
    .csv("hp.tsv") \
    .coalesce(1) \
    .write.mode("overwrite") \
    .parquet(output_name + "/HPO_ontology")