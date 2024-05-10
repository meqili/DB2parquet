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
parser.add_argument('--hgmd_version', required=True,
                    help='hgmd_version, eg. HGMD2024Q1')
args = parser.parse_args()

# Create spark session
spark = (
    pyspark.sql.SparkSession.builder.appName("DB2parquet")
    .getOrCreate()
    )
# Register so that glow functions like read vcf work with spark. Must be run in spark shell or in context described in help
spark = glow.register(spark)

# parameter configuration
hgmd_version = args.hgmd_version + "_"
gene_lite_file = "hg38_" + hgmd_version + "gene_lite.VWB.txt"
gene_sorted_file = "hg38_" + hgmd_version + "gene_sorted.VWB.txt"
variant_file = "hg38_" + hgmd_version + "variant.VWB.txt"

# main 
from pyspark.sql.functions import col, regexp_replace, split, regexp_extract
from pyspark.sql.types import LongType, FloatType

# gene_lite_file
spark.read.options(inferSchema=True,sep="\t",header=True,nullValue=".") \
    .csv(gene_lite_file) \
    .withColumnRenamed("Chr", "chromosome") \
    .withColumn('chromosome', regexp_replace('chromosome', 'chr', '')) \
    .withColumn("start", col("Start").cast(LongType())) \
    .withColumn("end", col("End").cast(LongType())) \
    .withColumn("split_c0", split(col("#EntrezGeneID_GeneSymbol"), "_")) \
    .withColumn("entrez_gene_id",col("split_c0").getItem(0)) \
    .withColumn("symbol",col("split_c0").getItem(1)) \
    .withColumn("variant_class",split(col("Phenotypes"), ",")) \
    .drop("Chr", "Phenotypes", "split_c0", "#EntrezGeneID_GeneSymbol") \
    .write.mode("overwrite") \
    .parquet("hg38_" + hgmd_version + "gene_lite")

# gene_sorted_file
spark.read.options(inferSchema=True,sep="\t",header=True, nullValue=".") \
    .csv(gene_sorted_file) \
    .withColumnRenamed("Chr", "chromosome") \
    .withColumn("chromosome", regexp_replace("chromosome", "chr", "")) \
    .withColumn("start", col("Start").cast(LongType())) \
    .withColumn("end", col("End").cast(LongType())) \
    .withColumn("split_c0", split(col("#EntrezGeneID_GeneSymbol"), "_")) \
    .withColumn("entrez_gene_id",col("split_c0").getItem(0)) \
    .withColumn("symbol",col("split_c0").getItem(1)) \
    .withColumn("split_c4",split(col("Phenotypes"), "~")) \
    .withColumn("DM", split(regexp_extract(col("split_c4").getItem(0), "^DM\\[([^\\]]*)?\\]?", 1), ",")) \
    .withColumn("DM?", split(regexp_extract(col("split_c4").getItem(1), "^DM\\?\\[([^\\]]*)?\\]?", 1), ",")) \
    .withColumn("DP", split(regexp_extract(col("split_c4").getItem(2), "^DP\\[([^\\]]*)?\\]?", 1), ",")) \
    .withColumn("DFP", split(regexp_extract(col("split_c4").getItem(3), "^DFP\\[([^\\]]*)?\\]?", 1), ",")) \
    .withColumn("FP", split(regexp_extract(col("split_c4").getItem(4), "^FP\\[([^\\]]*)?\\]?", 1), ",")) \
    .withColumn("R", split(regexp_extract(col("split_c4").getItem(5), "^R\\[([^\\]]*)?\\]?", 1), ",")) \
    .drop("Chr", "Phenotypes", "split_c0", "#EntrezGeneID_GeneSymbol", "split_c4") \
    .write.mode("overwrite") \
    .parquet("hg38_" + hgmd_version + "gene_sorted")

# variant_file
df = spark.read.options(inferSchema=True,sep="\t",header=True, nullValue=".") \
        .csv(variant_file) \
        .withColumnRenamed("#Chr", "chromosome") \
        .withColumn("chromosome", regexp_replace("chromosome", "chr", "")) \
        .withColumn("start", col("Start").cast(LongType())) \
        .withColumn("end", col("End").cast(LongType())) \
        .withColumnRenamed("Ref", "reference") \
        .withColumnRenamed("Alt", "alternate")
for c in df.columns :
    if hgmd_version in c:
        if "END" in c:
            new_c = "variant_end"
        elif "GENE" in c:
            new_c = "symbol"
        elif "CLASS" in c:
            new_c = "variant_class"
        else:
            new_c = c.replace(hgmd_version, "").lower()
        df = df.withColumnRenamed(c, new_c)
df.withColumn("rankscore", col("rankscore").cast(FloatType())) \
    .withColumn("variant_end", col("variant_end").cast(LongType())) \
    .withColumn("svlen", col("svlen").cast(LongType())) \
    .write.mode("overwrite") \
    .parquet("hg38_" + hgmd_version + "variant")